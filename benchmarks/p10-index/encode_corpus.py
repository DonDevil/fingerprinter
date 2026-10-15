"""P10 audit: encode a P9 corpus into a local vector cache (real vectors for the index study).

Uses the production decode, preprocessing and model of spec ``dinov2-seg/2``
(``fingerprinter.media.decode``/``preprocess``, ``fingerprinter.model.Embedder``)
in FP32 — the P9 default; CUDA FP32 equals CPU FP32 (cosine 1.0 on C-PREC,
P9 validation §7.3). It writes neither Scylla nor MinIO: one ``.npz`` per item
with ``segment_index`` int32 [N], ``pts_s`` float64 [N] and ``vectors``
float32 [N, 768] (L2-normalized), plus ``index.json`` (corpus manifest SHA-256,
spec digest, device, per-item segment count). Items already cached with the
same source SHA-256 are skipped, so a run can be resumed.

The cache lives outside git (``~/anti_piracy/datasets/p10/vectors/<corpus>/``).

    env/bin/python benchmarks/p10-index/encode_corpus.py --corpus ~/anti_piracy/datasets/p9/l-pairs \
        --out ~/anti_piracy/datasets/p10/vectors/l-pairs [--device cuda]
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]
CHUNK = 64
BATCH = 16


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from fingerprinter.media import decode, preprocess
    from fingerprinter.model import Embedder
    from fingerprinter.spec import dinov2_seg_v2

    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--kinds", default="video,clip,target")
    args = ap.parse_args()

    mpath = args.corpus / "manifest.json"
    manifest = json.loads(mpath.read_text())
    kinds = set(args.kinds.split(","))
    items = [i for i in manifest["items"] if i.get("kind", "video") in kinds and not
             str(i.get("expect", "READY")).startswith("FAILED")]
    spec = dinov2_seg_v2()
    emb = Embedder(args.device, "float32")
    args.out.mkdir(parents=True, exist_ok=True)
    idx_path = args.out / "index.json"
    index = json.loads(idx_path.read_text()) if idx_path.exists() else {"items": {}}
    index.update({"corpus": str(args.corpus), "manifest_sha256": hashlib.sha256(mpath.read_bytes()).hexdigest(),
                  "spec": "dinov2-seg/2", "spec_digest": str(spec.digest),
                  "device": args.device, "compute_dtype": "float32", "batch": BATCH})
    t_start = time.monotonic()
    for n, it in enumerate(items):
        name = it["file"]
        key = name.replace("/", "__")
        prev = index["items"].get(name)
        if prev and prev.get("sha256") == it.get("sha256") and (args.out / f"{key}.npz").exists():
            continue
        t0 = time.monotonic()
        ks, ts, vecs, chunk = [], [], [], []
        for f in decode(args.corpus / name, spec):
            ks.append(f.segment_index)
            ts.append(f.pts_s)
            chunk.append(preprocess(f.rgb))
            if len(chunk) >= CHUNK:
                x = np.stack(chunk)
                vecs.append(np.concatenate([emb.embed(x[i:i + BATCH]) for i in range(0, len(x), BATCH)]))
                chunk.clear()
        if chunk:
            x = np.stack(chunk)
            vecs.append(np.concatenate([emb.embed(x[i:i + BATCH]) for i in range(0, len(x), BATCH)]))
        v = np.concatenate(vecs).astype(np.float32) if vecs else np.zeros((0, 768), np.float32)
        np.savez(args.out / f"{key}.npz", segment_index=np.asarray(ks, np.int32),
                 pts_s=np.asarray(ts, np.float64), vectors=v)
        index["items"][name] = {"sha256": it.get("sha256"), "segments": int(len(ks)),
                                "seconds": round(time.monotonic() - t0, 2)}
        idx_path.write_text(json.dumps(index, indent=1, sort_keys=True))
        print(f"[{n + 1}/{len(items)}] {name}: {len(ks)} segments in {time.monotonic() - t0:.1f}s", flush=True)
    index["total_seconds_this_run"] = round(time.monotonic() - t_start, 1)
    idx_path.write_text(json.dumps(index, indent=1, sort_keys=True))


if __name__ == "__main__":
    main()
