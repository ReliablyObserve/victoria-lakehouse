package pmeta

import (
	"fmt"
	"hash/fnv"
	"math"
	"math/bits"
)

// hll is a minimal HyperLogLog cardinality estimator (in-house, no deps). It
// gives an approximate distinct-count for high-cardinality fields so the catalog
// can answer "≈ N distinct" instead of enumerating or scanning.
//
// Precision p → m = 2^p registers; standard error ≈ 1.04/√m (p=14 → ~0.81%).
// Uses a stable 64-bit hash (fnv-1a + a splitmix64 finalizer for avalanche) so
// estimates are deterministic and the sketch is mergeable across processes —
// merging is lossless register-max, so unioning per-partition sketches does NOT
// compound the error. Registers are dense (2^p bytes); precision trades accuracy
// for memory.
type hll struct {
	p   uint8
	reg []uint8
}

const defaultHLLPrecision = 14

// newHLL returns an empty sketch at the given precision (clamped to [4,18]).
func newHLL(p uint8) *hll {
	if p < 4 {
		p = 4
	}
	if p > 18 {
		p = 18
	}
	return &hll{p: p, reg: make([]uint8, 1<<p)}
}

// hllHash hashes a value to 64 bits. fnv-1a is fast and stable; the splitmix64
// finalizer fixes fnv's weak avalanche so the index/rho bits are well-distributed.
func hllHash(s string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	x := h.Sum64()
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// add folds a value into the sketch.
func (h *hll) add(s string) {
	x := hllHash(s)
	idx := x >> (64 - h.p)
	// Remaining bits after the index; the guard bit bounds rho to ≤ 64-p+1 even
	// when the substring is all zeros.
	w := (x << h.p) | (1 << (h.p - 1))
	rho := uint8(bits.LeadingZeros64(w)) + 1
	if rho > h.reg[idx] {
		h.reg[idx] = rho
	}
}

// regSums returns the inverse-power sum Σ 2^-reg[j] and the empty-register count.
func (h *hll) regSums() (sum float64, zeros float64) {
	for _, r := range h.reg {
		sum += 1.0 / float64(uint64(1)<<r)
		if r == 0 {
			zeros++
		}
	}
	return sum, zeros
}

func alphaM(m float64) float64 { return 0.7213 / (1 + 1.079/m) }

// estimate returns the approximate distinct-count. It uses the LogLog-Beta
// estimator (Qin/Kim/Tung 2016) at p=14 — a table-free polynomial that matches
// HLL++ (Heule et al. 2013) accuracy across the full cardinality range, including
// the mid-range where plain HLL (Flajolet 2007) is biased — and falls back to the
// classic estimator (+linear-counting) at other precisions. See hll_test.go for
// the HLL-vs-LogLog-Beta accuracy comparison.
func (h *hll) estimate() uint64 {
	if h.p == 14 {
		return h.estimateBeta()
	}
	return h.estimateClassic()
}

// estimateClassic is the original HLL estimator (Flajolet 2007) with 64-bit-hash
// (no large-range correction) and linear-counting in the small range. Kept for
// the accuracy comparison.
func (h *hll) estimateClassic() uint64 {
	m := float64(len(h.reg))
	sum, zeros := h.regSums()
	est := alphaM(m) * m * m / sum
	if est <= 2.5*m && zeros > 0 {
		est = m * math.Log(m/zeros) // linear counting
	}
	return uint64(est + 0.5)
}

// betaP14 is the LogLog-Beta bias-correction polynomial fitted for p=14
// (Qin/Kim/Tung 2016, Table 1). ez is the number of empty registers.
func betaP14(ez float64) float64 {
	z := math.Log(ez + 1)
	return -0.370393911*ez +
		0.070471823*z +
		0.17393686*z*z +
		0.16339839*z*z*z +
		-0.09237745*z*z*z*z +
		0.03738027*z*z*z*z*z +
		-0.005384159*z*z*z*z*z*z +
		0.00042419*z*z*z*z*z*z*z
}

// estimateBeta is the LogLog-Beta estimator: bias is absorbed by betaP14(ez) in
// the denominator, so there is no separate linear-counting branch and no
// empirical bias table — accurate from 0 to well past m.
func (h *hll) estimateBeta() uint64 {
	m := float64(len(h.reg))
	sum, ez := h.regSums()
	est := alphaM(m) * m * (m - ez) / (sum + betaP14(ez))
	if est < 0 {
		est = 0
	}
	return uint64(est + 0.5)
}

// merge folds another sketch (same precision) into this one via register-max.
func (h *hll) merge(o *hll) error {
	if h.p != o.p {
		return fmt.Errorf("hll: precision mismatch %d vs %d", h.p, o.p)
	}
	for i, r := range o.reg {
		if r > h.reg[i] {
			h.reg[i] = r
		}
	}
	return nil
}

// MarshalBinary encodes the sketch as: precision(1) | registers(2^p).
func (h *hll) MarshalBinary() []byte {
	out := make([]byte, 1+len(h.reg))
	out[0] = h.p
	copy(out[1:], h.reg)
	return out
}

// unmarshalHLL decodes a sketch, validating the precision + length so arbitrary
// bytes can't over-allocate or panic (fuzz-safe).
func unmarshalHLL(b []byte) (*hll, error) {
	if len(b) < 1 {
		return nil, fmt.Errorf("hll: short buffer")
	}
	p := b[0]
	if p < 4 || p > 18 {
		return nil, fmt.Errorf("hll: bad precision %d", p)
	}
	want := 1 << p
	if len(b)-1 != want {
		return nil, fmt.Errorf("hll: registers len %d, want %d", len(b)-1, want)
	}
	h := &hll{p: p, reg: make([]uint8, want)}
	copy(h.reg, b[1:])
	return h, nil
}
