"""P8's different-encoding set, rebuilt with P8's exact ffmpeg commands (validation §8, run and reported).

crawler2 ``benchmarks/p8-media/dedup_bench.py`` generates, at run time, one 6 s
320x240 ``testsrc`` + temporal-noise source as H.264 200k / 900k, a Matroska
remux and a fragmented-MP4 repack of the 900k stream, and VP8 WebM (HLS
renditions have no ``ContentId`` in P8, P8-D6, and are out of scope). The
commands are copied here (crawler2 is read-only); nothing in crawler2 runs.

    env/bin/python benchmarks/p10-index/p8_set.py build --out ~/anti_piracy/datasets/p10/p8-set
    env/bin/python benchmarks/p10-index/encode_corpus.py --corpus ~/anti_piracy/datasets/p10/p8-set \
        --out ~/anti_piracy/datasets/p10/vectors/p8-set --kinds video
    env/bin/python benchmarks/p10-index/p8_set.py eval --out ~/anti_piracy/datasets/p10/p8-set \
        --vectors ~/anti_piracy/datasets/p10/vectors/p8-set --report benchmarks/p10-index/results/p8-set.json
"""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import subprocess
import sys
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))

FF = ["ffmpeg", "-hide_banner", "-loglevel", "error", "-y"]
EXACT = ["-fflags", "+bitexact", "-flags:v", "+bitexact", "-map_metadata", "-1"]


def build(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    src = ["-f", "lavfi", "-i", "testsrc=duration=6:size=320x240:rate=25", "-vf", "noise=alls=25:allf=t"]
    x264 = ["-c:v", "libx264", "-pix_fmt", "yuv420p", "-g", "25", "-preset", "veryfast"]

    def ff(*args: str) -> None:
        subprocess.run([*FF, *args], check=True)

    ff(*src, *x264, "-b:v", "200k", *EXACT, str(out / "low.mp4"))
    ff(*src, *x264, "-b:v", "900k", *EXACT, str(out / "high.mp4"))
    ff("-i", str(out / "high.mp4"), "-c", "copy", *EXACT, str(out / "high.mkv"))
    ff(*src, "-c:v", "libvpx", "-b:v", "500k", *EXACT, str(out / "vp8.webm"))
    ff("-i", str(out / "high.mp4"), "-c", "copy", "-movflags", "frag_keyframe+empty_moov", *EXACT,
       str(out / "frag.mp4"))
    items = [{"file": n, "kind": "video", "sha256": hashlib.sha256((out / n).read_bytes()).hexdigest()}
             for n in ("low.mp4", "high.mp4", "high.mkv", "vp8.webm", "frag.mp4")]
    (out / "manifest.json").write_text(json.dumps({"corpus": "P8 different-encoding set (rebuilt)",
                                                   "items": items}, indent=1))


def evaluate(out: Path, vectors: Path, report: Path) -> None:
    from fingerprinter.verify import Vectors, VerifierParams, equivalence

    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    params = VerifierParams.from_document(tune["final_parameter_set"]["params"])
    items = json.loads((out / "manifest.json").read_text())["items"]
    vec = {}
    for it in items:
        z = np.load(vectors / (it["file"].replace("/", "__") + ".npz"))
        vec[it["file"]] = Vectors(z["segment_index"], z["pts_s"], z["vectors"].astype(np.float32))
    pairs = {}
    for x, y in itertools.combinations(sorted(vec), 2):
        eq = equivalence(vec[x], vec[y], params)
        pairs[f"{x}|{y}"] = {"linked": eq.linked, "reason": eq.reason, "coverage": [eq.coverage_a, eq.coverage_b]}
    doc = {"benchmark": "P10-P8-SET", "segments_per_item": {k: len(v) for k, v in vec.items()},
           "pairs": pairs, "linked": sum(p["linked"] for p in pairs.values()), "total_pairs": len(pairs),
           "note": "all pairs are the same 6 s content; the plan expected them below the minimum verifiable duration"}
    report.write_text(json.dumps(doc, indent=1, sort_keys=True))
    print(json.dumps({k: doc[k] for k in ("segments_per_item", "linked", "total_pairs")}))


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=("build", "eval"))
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--vectors", type=Path)
    ap.add_argument("--report", type=Path)
    a = ap.parse_args()
    if a.cmd == "build":
        build(a.out)
    else:
        evaluate(a.out, a.vectors, a.report)


if __name__ == "__main__":
    main()
