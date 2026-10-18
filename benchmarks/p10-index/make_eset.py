"""E-SET (P10-D18, validation §8): level-5 equivalence test media from S-SRC excerpts.

For each of ``--excerpts`` excerpts (60-120 s, seeded positions spread over the
7 openly licensed S-SRC films; never ``Blast.mp4``):

- a 480p H.264 mezzanine cut (``mezz``), then six **variants** of the same
  excerpt — H.264 low bitrate (400 kb/s), H.264 high bitrate (3 Mb/s), MKV
  remux (stream copy of the high-bitrate file), fragmented MP4 with 1 s and
  with 2 s fragments (stream copy), VP9 WebM: every pair of variants of one
  excerpt is *equivalent* (same excerpt, different encoding/packaging);
- negatives: the same excerpt **shifted by 30 s** (same duration), a **50 %
  sub-clip**, and (implicitly) every variant of every other excerpt.

Labels are provenance only. Output (outside git):
``~/anti_piracy/datasets/p10/e-set/{media/, manifest.json}``.

    env/bin/python benchmarks/p10-index/make_eset.py --data ~/anti_piracy/datasets/p9 \
        --out ~/anti_piracy/datasets/p10/e-set --excerpts 20
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from pathlib import Path

import numpy as np

VARIANTS = {
    "h264-400k": ["-c:v", "libx264", "-preset", "veryfast", "-b:v", "400k", "-maxrate", "400k", "-bufsize", "800k"],
    "h264-3m": ["-c:v", "libx264", "-preset", "veryfast", "-b:v", "3M"],
    "vp9": ["-c:v", "libvpx-vp9", "-deadline", "realtime", "-cpu-used", "8", "-b:v", "1M", "-row-mt", "1"],
}


def run(cmd: list[str]) -> None:
    subprocess.run(["nice", "-n", "10", "ffmpeg", "-nostdin", "-loglevel", "error", "-y", *cmd], check=True)


def sha(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--excerpts", type=int, default=20)
    ap.add_argument("--seed", type=int, default=8)
    a = ap.parse_args()
    lp = json.loads((a.data / "l-pairs" / "manifest.json").read_text())
    films = [(i["film"], (a.data / "l-pairs" / i["file"]).resolve(), float(i["probe"]["duration_s"]))
             for i in lp["items"] if i["kind"] == "target"]
    rng = np.random.default_rng(a.seed)
    media = a.out / "media"
    media.mkdir(parents=True, exist_ok=True)
    items = []
    for e in range(a.excerpts):
        film, path, dur = films[e % len(films)]
        length = float(rng.integers(60, 121))
        start = float(rng.uniform(20.0, max(dur - length - 60.0, 21.0)))
        base = f"e{e:02d}-{film}"
        mezz = media / f"{base}-mezz.mp4"
        vf = ["-vf", "scale=-2:480,format=yuv420p", "-an", "-map", "0:v:0"]
        if not mezz.exists():
            run(["-ss", f"{start:.3f}", "-t", f"{length:.3f}", "-i", str(path), *vf, "-c:v", "libx264", "-preset",
                 "veryfast", "-crf", "18", str(mezz)])
        outs = {}
        for name, args in VARIANTS.items():
            ext = "webm" if name == "vp9" else "mp4"
            out = media / f"{base}-{name}.{ext}"
            if not out.exists():
                run(["-i", str(mezz), *args, "-an", str(out)])
            outs[name] = out
        remux = media / f"{base}-remux.mkv"
        if not remux.exists():
            run(["-i", str(outs["h264-3m"]), "-c", "copy", str(remux)])
        outs["remux-mkv"] = remux
        for frag in (1, 2):
            out = media / f"{base}-fmp4-{frag}s.mp4"
            if not out.exists():
                run(["-i", str(outs["h264-3m"]), "-c", "copy",
                     "-movflags", "+frag_keyframe+empty_moov+default_base_moof",
                     "-frag_duration", str(frag * 1_000_000), str(out)])
            outs[f"fmp4-{frag}s"] = out
        shifted = media / f"{base}-shift30.mp4"
        if not shifted.exists():
            run(["-ss", f"{start + 30.0:.3f}", "-t", f"{length:.3f}", "-i", str(path), *vf, "-c:v", "libx264",
                 "-preset",
                 "veryfast", "-b:v", "3M", str(shifted)])
        sub = media / f"{base}-half.mp4"
        if not sub.exists():
            run(["-ss", f"{start:.3f}", "-t", f"{length / 2:.3f}", "-i", str(path), *vf, "-c:v", "libx264", "-preset",
                 "veryfast", "-b:v", "3M", str(sub)])
        for name, out in outs.items():
            items.append({"file": f"media/{out.name}", "kind": "video", "excerpt": e, "film": film, "role": "variant",
                          "variant": name, "reel_start_s": None, "start_s": round(start, 3), "seconds": length,
                          "sha256": sha(out)})
        items.append({"file": f"media/{shifted.name}", "kind": "video", "excerpt": e, "film": film,
                      "role": "negative_shift30", "start_s": round(start + 30.0, 3), "seconds": length,
                      "sha256": sha(shifted)})
        items.append({"file": f"media/{sub.name}", "kind": "video", "excerpt": e, "film": film,
                      "role": "negative_half", "start_s": round(start, 3), "seconds": length / 2, "sha256": sha(sub)})
        print(f"[{e + 1}/{a.excerpts}] {base}: {length:.0f}s at {start:.1f}s", flush=True)
    manifest = {"corpus": "E-SET",
                "definition": "validation §8 / P10-D18; labels = provenance (same excerpt, different "
                "encoding or packaging = equivalent; shifted 30 s, 50 % sub-clip, other excerpts = not)",
                "seed": a.seed, "variants": [*VARIANTS, "remux-mkv", "fmp4-1s", "fmp4-2s"], "items": items,
                "licence_note": "derived from the S-SRC openly licensed films (P9 data-preparation §7)"}
    (a.out / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True))
    print(json.dumps({"items": len(items)}))


if __name__ == "__main__":
    main()
