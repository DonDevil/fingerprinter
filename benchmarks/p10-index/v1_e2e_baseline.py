"""P10 I-1 (a): the reproducible V1 end-to-end baseline (validation.md §2.1, P10-D17).

Runs the **unchanged V1 worker handler** —
``worker.matching_handler.build_matching_handler(acquirer, engine, registry,
MatcherConfig())`` — in-process, one ``Job`` per labelled (clip, target)
pair: acquisition over HTTP from a local fixture server → V1
``DINOv2EmbeddingEngine`` segment embedding (V1 ``SegmentSamplingConfig``,
5 s, ``fps=1/5``) → target resolution from the V1 ``TargetRegistry`` (Redis,
build-on-miss from the S-SRC film) → coarse screen 0.60 → ``match_segments``
(0.90 / 3 / 1 / 2) → ``matching.aggregation`` → ``Result``.

No V1 code is modified. The only test configuration is the one V1's own
integration tests use: ``MediaAcquirer(allow_private_networks=True)`` so the
SSRF guard admits the loopback fixture server, and Redis db 15.

    env/bin/python benchmarks/p10-index/v1_e2e_baseline.py \
        --data ~/anti_piracy/datasets/p9 --device cuda \
        --scratch ~/anti_piracy/datasets/p10/runs/v1-e2e \
        --out benchmarks/p10-index/results/v1-e2e-l-pairs.json

Per-pair outputs (decision, confidence, evidence JSON, timings) go to
``<scratch>/pairs.jsonl``; the summary JSON is the committed evidence.
"""

from __future__ import annotations

import argparse
import http.server
import json
import os
import platform
import subprocess
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from corpus import by_fold, confusion, load_lpairs  # noqa: E402

CONTENT_TYPES = {".mp4": "video/mp4", ".mkv": "video/x-matroska", ".webm": "video/webm",
                 ".mov": "video/quicktime"}


class _Files(http.server.BaseHTTPRequestHandler):
    root: Path

    def log_message(self, *args) -> None:
        pass

    def do_GET(self) -> None:
        path = (self.root / self.path.lstrip("/")).resolve()
        if not path.is_file() or self.root.resolve() not in path.parents:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        size = path.stat().st_size
        self.send_response(200)
        self.send_header("Content-Type", CONTENT_TYPES.get(path.suffix.lower(), "application/octet-stream"))
        self.send_header("Content-Length", str(size))
        self.end_headers()
        with path.open("rb") as fh:
            while chunk := fh.read(1 << 20):
                self.wfile.write(chunk)


def serve(root: Path) -> tuple[http.server.ThreadingHTTPServer, str]:
    handler = type("Files", (_Files,), {"root": root})
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"


def pairs_first(pop) -> str:
    return pop.pairs[0].item


def run_job(handler, job, failures):
    try:
        res = handler(job)
        return res.decision, res.confidence, res.evidence, None
    except failures as exc:
        return "worker_failure", None, None, f"{type(exc).__name__}: {exc}"


def git_head() -> str:
    try:
        return subprocess.run(["git", "-C", str(ROOT), "rev-parse", "HEAD"], capture_output=True,
                              text=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--device", default="cuda")
    ap.add_argument("--redis-url", default="redis://localhost:6379/15")
    ap.add_argument("--scratch", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--limit", type=int, help="first N pairs only (harness check, never a gate)")
    a = ap.parse_args()

    import torch
    import transformers
    from redis import Redis

    from acquisition import MediaAcquirer
    from embedding.dinov2_engine import DINOv2EmbeddingEngine
    from matching.aggregation import DINOV2_TEMPORAL_TECHNIQUE
    from matching.config import MatcherConfig
    from target.cache import FilesystemEmbeddingCache
    from target.registry import TargetRegistry
    from target.segment_cache import FilesystemSegmentEmbeddingCache
    from work_queue.jobs import Job
    from work_queue.results import ResultDecision
    from worker.fingerprint_worker import PermanentFailure, TransientFailure
    from worker.matching_handler import build_matching_handler

    pop = load_lpairs(a.data)
    corpus = a.data / "l-pairs"
    a.scratch.mkdir(parents=True, exist_ok=True)
    redis = Redis.from_url(a.redis_url, decode_responses=True)
    redis.flushdb()
    registry = TargetRegistry(
        redis,
        FilesystemEmbeddingCache(a.scratch / "embedding-cache"),
        FilesystemSegmentEmbeddingCache(a.scratch / "segment-cache"),
    )
    for film, fname in pop.target_file.items():
        registry.register_target(film, "1", str((corpus / "targets" / fname).resolve()))
    engine = DINOv2EmbeddingEngine(device=a.device)
    acquirer = MediaAcquirer(allow_private_networks=True)
    config = MatcherConfig()
    handler = build_matching_handler(acquirer, engine, registry, config)
    server, base = serve(corpus / "clips")
    # warm the ffprobe binary and libraries once: V1's acquisition probe has a 5 s
    # timeout that a cold first invocation on this HDD host can exceed
    subprocess.run(["ffprobe", "-v", "error", str(corpus / "clips" / pairs_first(pop))], check=False)

    pairs = pop.pairs[: a.limit] if a.limit else pop.pairs
    out_rows = []
    started = time.time()
    with (a.scratch / "pairs.jsonl").open("w") as log:
        for n, p in enumerate(pairs):
            job = Job(job_id=f"v1e2e-{n:04d}", media_evidence_id=p.item, media_url=f"{base}/{p.item}",
                      media_type="video", source_domain="fixture.local", target_id=p.target,
                      target_version="1", techniques=(DINOV2_TEMPORAL_TECHNIQUE,), max_attempts=1)
            t0 = time.time()
            decision, conf, evidence, failure = run_job(handler, job, (PermanentFailure, TransientFailure))
            strict = decision
            retried = None
            if failure and "media probe timed out" in failure:
                # environmental (host I/O contention), not a property of the clip: one retry,
                # reported separately; `decision_strict` keeps V1's worker semantics (no retry)
                retried = failure
                decision, conf, evidence, failure = run_job(handler, job, (PermanentFailure, TransientFailure))
            row = {"n": n, "clip": p.item, "target": p.target, "label": int(p.label), "transform": p.transform,
                   "fold": p.fold, "decision": decision, "decision_strict": strict, "retried_after": retried,
                   "confidence": conf, "evidence": evidence,
                   "failure": failure, "seconds": round(time.time() - t0, 3)}
            log.write(json.dumps(row, sort_keys=True) + "\n")
            log.flush()
            out_rows.append(row)
            if n % 20 == 0:
                print(f"[{n}/{len(pairs)}] {time.time() - started:.0f}s {p.item} -> {p.target}: {decision}",
                      flush=True)
    server.shutdown()

    def predicted(r):  # a PROCESSING_FAILURE / worker failure is not a positive decision
        return r["decision"] == ResultDecision.MATCH

    overall = confusion([(bool(r["label"]), predicted(r)) for r in out_rows])
    overall_strict = confusion([(bool(r["label"]), r["decision_strict"] == ResultDecision.MATCH) for r in out_rows])
    per_transform = {}
    for t in sorted({r["transform"] for r in out_rows}):
        rows = [r for r in out_rows if r["transform"] == t]
        per_transform[t] = confusion([(bool(r["label"]), predicted(r)) for r in rows])
    per_target = {}
    for f in pop.films:
        rows = [r for r in out_rows if r["target"] == f]
        per_target[f] = confusion([(bool(r["label"]), predicted(r)) for r in rows])
    per_fold = {}
    folds = by_fold([p for p in pairs])
    for fold in sorted(folds):
        keys = {(q.item, q.target) for q in folds[fold]}
        rows = [r for r in out_rows if (r["clip"], r["target"]) in keys]
        per_fold[fold] = confusion([(bool(r["label"]), predicted(r)) for r in rows])
    failures = defaultdict(int)
    for r in out_rows:
        if r["decision"] not in (ResultDecision.MATCH, ResultDecision.NO_MATCH):
            failures[r["decision"]] += 1
    report = {
        "benchmark": "P10-I1a-V1-E2E-BASELINE",
        "population": "L-PAIRS" if not a.limit else f"L-PAIRS first {a.limit} pairs (harness check, not a gate)",
        "gate_eligible": not a.limit,
        "code_path": "worker.matching_handler.build_matching_handler(MediaAcquirer(allow_private_networks=True), "
                     "DINOv2EmbeddingEngine(device), TargetRegistry(redis db 15, fs caches), MatcherConfig())",
        "fingerprinter_commit": git_head(),
        "v1_matcher_config": config.__dict__,
        "v1_model": {"id": engine.model_id, "revision": engine.model_revision},
        "v1_segment_sampling": str(engine.segment_sampling_config),
        "versions": {"python": platform.python_version(), "torch": torch.__version__,
                     "transformers": transformers.__version__},
        "device": a.device,
        "gpu": torch.cuda.get_device_name(0) if a.device == "cuda" and torch.cuda.is_available() else None,
        "manifests_sha256": pop.manifests,
        "pairs": len(out_rows),
        "positive_decision": "Result.decision == match (PROCESSING_FAILURE and worker failures count as no decision)",
        "overall": overall,
        "overall_strict_v1_worker_semantics": overall_strict,
        "probe_timeout_retries": sum(1 for r in out_rows if r["retried_after"]),
        "per_transform": per_transform,
        "per_target_film": per_target,
        "per_outer_fold_heldout": per_fold,
        "non_decisions": dict(failures),
        "wall_seconds": round(time.time() - started, 1),
        "per_pair_output": str(a.scratch / "pairs.jsonl"),
        "host": platform.node(),
        "pid": os.getpid(),
    }
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    print(json.dumps({"overall": overall, "non_decisions": dict(failures)}, indent=1))


if __name__ == "__main__":
    main()
