package s3reader

import (
	"context"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
)

// StoreClock is the object store's clock, learned from the Date header of its
// responses and advanced with the local monotonic clock in between. Decisions
// that must not depend on a pod's own wall clock (whether a file is past its
// retention) read it instead of time.Now. The Date header has a resolution of one
// second.
type StoreClock struct {
	offset atomic.Int64 // store time minus local time at the last observation, ns
	seen   atomic.Bool
}

// install adds the middleware that feeds the clock from every response.
func (c *StoreClock) install(o *s3.Options) {
	o.APIOptions = append(o.APIOptions, func(stack *middleware.Stack) error {
		return stack.Deserialize.Add(middleware.DeserializeMiddlewareFunc("LakehouseStoreDate",
			func(ctx context.Context, in middleware.DeserializeInput, next middleware.DeserializeHandler) (middleware.DeserializeOutput, middleware.Metadata, error) {
				out, md, err := next.HandleDeserialize(ctx, in)
				if resp, ok := out.RawResponse.(*smithyhttp.Response); ok && resp != nil {
					c.Observe(resp.Header.Get("Date"))
				}
				return out, md, err
			}), middleware.Before)
	})
}

// Observe records a Date header value. An absent or unparsable value changes
// nothing. It runs on every response, so it takes no lock.
func (c *StoreClock) Observe(date string) {
	if date == "" {
		return
	}
	t, err := http.ParseTime(date)
	if err != nil {
		return
	}
	c.offset.Store(int64(time.Until(t)))
	c.seen.Store(true)
}

// Known reports whether any response has carried a Date header yet.
func (c *StoreClock) Known() bool { return c.seen.Load() }

// Now is the store's current time. Without any observation it is the local time.
func (c *StoreClock) Now() time.Time {
	return time.Now().Add(time.Duration(c.offset.Load()))
}

// StoreNow is the object store's current time (see StoreClock).
func (p *ClientPool) StoreNow() time.Time {
	if p.clock == nil {
		return time.Now()
	}
	return p.clock.Now()
}

// StoreClock returns the pool's observed store clock (never nil for a pool from
// NewClientPool).
func (p *ClientPool) StoreClock() *StoreClock { return p.clock }

// StoreClockKnown reports whether the store's clock has been observed.
func (p *ClientPool) StoreClockKnown() bool {
	return p.clock != nil && p.clock.Known()
}
