#!/usr/bin/env python3
# ported-from loki-vl-proxy/bench/visual/montage.py@429f15b9 (sides renamed: base | PR | reference; no Loki-only placeholder)
"""Side-by-side montages (base | PR | reference) and a pixel-diff score base vs PR.

  montage.py OUT

Reads OUT/shots/<page>/<range>/{base,pr,ref}.png, writes OUT/montage/<page>-<range>.png
(downscaled, palette PNG, at most 300 KB) and OUT/pixeldiff.json (fraction of pixels whose colour
differs between base and PR by more than a small threshold; 0.0 = identical).
"""
import glob
import os
import sys

from PIL import Image, ImageChops, ImageDraw

from .vio import dump_json

W = 640  # width of each panel in the montage
MAX_BYTES = 300_000  # a montage hosted on the pr-visuals branch stays at or below this
TITLES = (("base", "base (main, before)"), ("pr", "PR (after)"), ("ref", "hot VictoriaLogs / VictoriaTraces (reference)"))


def load_image(path):
    with Image.open(path) as im:
        return im.convert("RGB")


def label(img, text):
    bar = Image.new("RGB", (img.width, 22), (30, 30, 30))
    ImageDraw.Draw(bar).text((6, 5), text, fill=(255, 255, 255))
    out = Image.new("RGB", (img.width, img.height + 22))
    out.paste(bar, (0, 0))
    out.paste(img, (0, 22))
    return out


def save_small(img, path, limit=MAX_BYTES):
    """Palette PNG; fewer colours, then a smaller image, until the file fits `limit`."""
    for scale in (1.0, 0.85, 0.7, 0.55, 0.4):
        im = img if scale == 1.0 else img.resize((int(img.width * scale), int(img.height * scale)), Image.LANCZOS)
        for colors in (128, 64, 32):
            im.quantize(colors=colors, method=Image.Quantize.MEDIANCUT, dither=Image.Dither.NONE).save(path, optimize=True)
            if os.path.getsize(path) <= limit:
                return
    raise SystemExit(f"{path} does not fit {limit} bytes")


def score(a, b):
    if a.size != b.size:
        return 1.0
    diff = ImageChops.difference(a, b).convert("L").point(lambda v: 255 if v > 24 else 0)
    return round(sum(1 for v in diff.tobytes() if v) / (a.width * a.height), 5)


def make(out):
    os.makedirs(os.path.join(out, "montage"), exist_ok=True)
    scores = {}
    for d in sorted(glob.glob(os.path.join(out, "shots", "*", "*"))):
        page, rng = d.split(os.sep)[-2:]
        # a page can also save a clip of the region it is about (<side>-clip.png): its own montage, <page>-<range>-zoom.png
        for suffix, name in (("", f"{page}-{rng}"), ("-clip", f"{page}-{rng}-zoom")):
            one(d, suffix, name, out, scores)
    dump_json(os.path.join(out, "pixeldiff.json"), scores)
    return scores


def one(d, suffix, name, out, scores):
    imgs = {n: load_image(os.path.join(d, f"{n}{suffix}.png")) for n, _ in TITLES if os.path.exists(os.path.join(d, f"{n}{suffix}.png"))}
    if "base" not in imgs or "pr" not in imgs:
        return
    if "ref" not in imgs:
        imgs["ref"] = Image.new("RGB", imgs["base"].size, (245, 245, 245))
        ImageDraw.Draw(imgs["ref"]).text((20, 20), "reference: not captured for this page", fill=(60, 60, 60))
    scores[name] = score(imgs["base"], imgs["pr"])
    h = int(imgs["base"].height * W / imgs["base"].width)
    tiles = [label(imgs[n].resize((W, h), Image.LANCZOS), t) for n, t in TITLES]
    m = Image.new("RGB", (W * 3 + 8, tiles[0].height), (255, 255, 255))
    for i, t in enumerate(tiles):
        m.paste(t, (i * (W + 4), 0))
    save_small(m, os.path.join(out, "montage", f"{name}.png"))


def main():
    for k, v in make(sys.argv[1]).items():
        print(f"{k}: {v}")


if __name__ == "__main__":
    main()
