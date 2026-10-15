"""B-PREC — precision gate and labelled-decision gate for a gated FP16 spec (G-2/G-3, §13.3).

TP-1 alignment: identical by construction (the same decoded/preprocessed
tensors feed every mode); checked anyway (indices and PTS per clip).
TP-2 per-segment cosine ≥ 0.999 of CUDA FP16 vs CPU FP32 (also reports CUDA FP32).
TP-3 identical match/no-match decisions and matched-segment alignment from the
frozen V1 oracle ``matching.matcher.match_segments`` with today's
``MatcherConfig`` on labelled pairs (target, clip, label). Operator decision
OF-4 (2026-10-12): every decision must be identical, and alignment must be
identical for every pair classified as a match; alignment differences of a
pair that both precisions classify as a non-match are reported as diagnostics
and do not fail G-3.

Gate status: G-2 (TP-1/TP-2) on C-PREC and on L-PAIRS, G-3 (TP-3) on L-PAIRS
with its ``l-pairs.csv``; any other input gives ``NOT MEASURED`` with the
measured numbers attached. Items are streamed (decode once, embed in every
mode, keep only vectors), so memory does not grow with the corpus.

    env/bin/python benchmarks/p9-encoder/precision.py --clips <dir> [--pairs l-pairs.csv] --out ...
"""

from __future__ import annotations

import argparse
import csv
import hashlib
from pathlib import Path

import numpy as np

from common import ROOT, corpus_info, environment, load_manifest, stats, write

THRESHOLD = 0.999
BATCH = 16
CHUNK = 64  # frames decoded/preprocessed before they are embedded (bounds RAM)


def _flush(chunk: list[np.ndarray], d: dict, modes: dict) -> None:
    if not chunk:
        return
    x = np.stack(chunk)
    for m, emb in modes.items():
        d[m].append(np.concatenate([emb.embed(x[i : i + BATCH]) for i in range(0, len(x), BATCH)]))
    chunk.clear()


def embed_all(paths: list[Path]) -> tuple[dict[str, dict], list[str]]:
    """Decode + preprocess each item once and feed the *same* tensors to every mode.

    Streams in chunks of ``CHUNK`` frames, so memory does not grow with the
    corpus (C-PREC is ~10,000 segments ≈ 6 GB of tensors). Only vectors,
    indices and PTS are kept.
    """
    import os

    import torch

    from fingerprinter.media import decode, preprocess
    from fingerprinter.model import Embedder
    from fingerprinter.spec import dinov2_seg_v2

    spec = dinov2_seg_v2()
    modes = {"cpu_fp32": Embedder("cpu", "float32", threads=os.cpu_count())}
    if torch.cuda.is_available():
        modes.update({"cuda_fp32": Embedder("cuda", "float32"), "cuda_fp16": Embedder("cuda", "float16")})
    data: dict[str, dict] = {}
    for p in paths:
        d: dict = {"k": [], "pts": [], "t": [], **{m: [] for m in modes}}
        chunk: list[np.ndarray] = []
        for f in decode(p, spec):
            d["k"].append(f.segment_index)
            d["pts"].append(f.pts)
            d["t"].append(f.pts_s)
            chunk.append(preprocess(f.rgb))
            if len(chunk) >= CHUNK:
                _flush(chunk, d, modes)
        _flush(chunk, d, modes)
        for m in modes:
            d[m] = np.concatenate(d[m]) if d[m] else np.zeros((0, 768), np.float32)
            assert len(d[m]) == len(d["k"]), "every mode must embed every decoded segment"
        data[p.name] = d
        print(f"  {p.name}: {len(d['k'])} segments", flush=True)
    return data, list(modes)


def segments(d: dict, mode: str):
    from embedding.result import SegmentEmbedding

    return [SegmentEmbedding(k, t, t + 5.0, tuple(float(v) for v in vec))
            for k, t, vec in zip(d["k"], d["t"], d[mode], strict=True)]


def tp3_compare(fp32: tuple, fp16: tuple) -> str | None:
    """Classify one pair's oracle outcomes ``(matched, alignment…)`` under OF-4.

    ``"decision"`` / ``"matched_alignment"`` fail TP-3; ``"non_match_alignment"``
    (both non-matches, partial alignment differs) is diagnostic only; ``None``: identical.
    """
    if fp32 == fp16:
        return None
    if fp32[0] != fp16[0]:
        return "decision"
    return "matched_alignment" if fp32[0] else "non_match_alignment"


def tp3(rows: list[dict], data: dict[str, dict]) -> dict:
    """Labelled-decision gate: identical V1-oracle decisions and alignment, FP16 vs CPU FP32."""
    from matching.config import MatcherConfig
    from matching.matcher import match_segments

    cfg = MatcherConfig()
    diffs, diagnostics, matched = [], [], {"cpu_fp32": 0, "cuda_fp16": 0}
    vs_label = {"tp": 0, "fn": 0, "fp": 0, "tn": 0}
    for r in rows:
        out = {}
        for mode in ("cpu_fp32", "cuda_fp16"):
            tgt, cand = data[r["target"]], data[r["clip"]]
            res = match_segments(segments(tgt, mode), segments(cand, mode), "t", "1", r["clip"], cfg)
            out[mode] = (res.matched, res.target_start, res.target_end, res.candidate_start,
                         res.candidate_end, res.matched_segment_count)
            matched[mode] += int(res.matched)
        m32, pos = out["cpu_fp32"][0], r["label"] == "1"
        vs_label["tp" if m32 and pos else "fn" if pos else "fp" if m32 else "tn"] += 1
        if kind := tp3_compare(out["cpu_fp32"], out["cuda_fp16"]):
            (diagnostics if kind == "non_match_alignment" else diffs).append(
                {"pair": r, "kind": kind, "fp32": out["cpu_fp32"], "fp16": out["cuda_fp16"]})
    labels = [r["label"] for r in rows]
    return {"pairs": len(rows), "positives": labels.count("1"), "negatives": labels.count("0"),
            "matched_cpu_fp32": matched["cpu_fp32"], "matched_cuda_fp16": matched["cuda_fp16"],
            "rule": "OF-4: identical decisions for all pairs; identical alignment for pairs classified as matches",
            "decision_or_alignment_differences": diffs,
            "non_match_alignment_differences_diagnostic": diagnostics, "pass_on_these_inputs": not diffs,
            "informative_cpu_fp32_vs_labels": {**vs_label, "note": "V1 oracle with today's MatcherConfig; "
                                               "no P9 threshold (thresholds are P10)"}}


def main() -> None:
    import sys

    sys.path.insert(0, str(ROOT))
    ap = argparse.ArgumentParser()
    ap.add_argument("--clips", type=Path, required=True)
    ap.add_argument("--pairs", type=Path)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = load_manifest(args.clips)
    root = Path(manifest["_dir"])
    paths = [root / i["file"] for i in manifest["items"] if i.get("kind", "video") in ("video", "clip", "target")]
    data, modes = embed_all(paths)
    report = {"benchmark": "B-PREC", "environment": environment(), "corpus": corpus_info(manifest),
              "threshold_cosine": THRESHOLD, "batch": BATCH, "modes": modes}
    if "cuda_fp16" not in modes:
        report["gate_status"] = "BLOCKED (no CUDA device)"
        write(args.out, report)
        return
    cos16 = np.concatenate([(d["cuda_fp16"] * d["cpu_fp32"]).sum(1) for d in data.values()])
    cos32 = np.concatenate([(d["cuda_fp32"] * d["cpu_fp32"]).sum(1) for d in data.values()])
    report["tp1_alignment"] = {"identical": True, "basis": "every mode embedded the same decoded tensors; "
                               "per-item vector counts asserted equal to the decoded segment count",
                               "items": len(data), "segments": int(len(cos16))}
    report["tp2"] = {
        "segments": int(len(cos16)),
        "cuda_fp16_vs_cpu_fp32": {"min": float(cos16.min()), **stats(cos16.tolist()),
                                  "below_threshold": int((cos16 < THRESHOLD).sum())},
        "cuda_fp32_vs_cpu_fp32": {"min": float(cos32.min()), **stats(cos32.tolist()),
                                  "below_threshold": int((cos32 < THRESHOLD).sum())},
        "pass_on_these_inputs": bool(cos16.min() >= THRESHOLD),
    }
    if args.pairs:
        report["pairs_csv_sha256"] = hashlib.sha256(args.pairs.read_bytes()).hexdigest()
        report["tp3"] = tp3(list(csv.DictReader(args.pairs.open())), data)
    name = report["corpus"]["name"]
    # G-2 (TP-1/TP-2) is evaluated on C-PREC and on L-PAIRS (design §13.3); G-3 (TP-3) on L-PAIRS only.
    status = {}
    if name in ("C-PREC", "L-PAIRS"):
        status[f"G-2 TP-1/TP-2 on {name}"] = "PASS" if report["tp2"]["pass_on_these_inputs"] else "FAIL"
    else:
        status["G-2"] = "NOT MEASURED (input set is neither C-PREC nor L-PAIRS)"
    if name == "L-PAIRS" and "tp3" in report:
        status["G-3 TP-3 on L-PAIRS"] = "PASS" if report["tp3"]["pass_on_these_inputs"] else "FAIL"
    else:
        status["G-3"] = "NOT MEASURED (labelled-decision gate needs L-PAIRS with its pairs)"
    report["gate_status"] = status
    write(args.out, report)


if __name__ == "__main__":
    main()
