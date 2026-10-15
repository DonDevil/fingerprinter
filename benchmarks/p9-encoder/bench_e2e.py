"""B-CPU / B-GPU-b / B-DEC — end-to-end encode throughput and stage times (design §13.2).

Full pipeline against the real stack: acquisition from a local fixture HTTP
server → decode → preprocess → inference → artifact upload → Scylla commit →
publication, with one encoder (device as given). Reports seconds of wall time
per minute of video, segments/s, per-stage sums (acquire, decode incl.
preprocess and queue waits, infer, commit) and peak RSS/VRAM. ``--decode-only``
measures B-DEC (ffmpeg select + rgb24 pipe alone, no model).

End-to-end has **no threshold** (P9-D17); the real-media baseline G-6 needs the
C-PREC corpus — other corpora are reported as measurements only.

    . scripts/p9-env.sh
    env/bin/python benchmarks/p9-encoder/bench_e2e.py --corpus <dir> --device cpu --out ...
"""

from __future__ import annotations

import argparse
import json
import resource
import threading
import time
import uuid
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from common import ROOT, corpus_info, environment, load_manifest, stats, videos, write


class _Quiet(SimpleHTTPRequestHandler):
    def log_message(self, *a: Any) -> None:
        pass


def decode_only(paths: list[Path]) -> dict[str, Any]:
    from fingerprinter.media import decode, probe
    from fingerprinter.spec import dinov2_seg_v2

    spec = dinov2_seg_v2()
    per = []
    for p in paths:
        pr = probe(p)
        t0 = time.perf_counter()
        n = sum(1 for _ in decode(p, spec))
        dt = time.perf_counter() - t0
        per.append({"file": p.name, "codec": pr.codec, "size": f"{pr.width}x{pr.height}",
                    "duration_s": pr.duration_s, "segments": n, "wall_s": round(dt, 3),
                    "video_s_per_wall_s": round((pr.duration_s or 0) / dt, 2) if dt else None})
    return {"items": per, "video_s_per_wall_s": stats([x["video_s_per_wall_s"] for x in per if x["video_s_per_wall_s"]])}


def main() -> None:  # noqa: PLR0915
    import sys

    sys.path.insert(0, str(ROOT))
    from prometheus_client import REGISTRY

    from fingerprinter.config import Settings
    from fingerprinter.encoder import Encoder
    from fingerprinter.media import probe
    from fingerprinter.publish import FAILED, READY
    from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile
    from fingerprinter.services import Services
    from fingerprinter.spec import RUNTIME_PROFILE, dinov2_seg_v2
    from tests.v2.conftest import wipe
    from tests.v2.helpers import request_env

    ap = argparse.ArgumentParser()
    ap.add_argument("--corpus", type=Path, required=True)
    ap.add_argument("--device", default="cpu")
    ap.add_argument("--decode-only", action="store_true")
    ap.add_argument("--limit", type=int)
    ap.add_argument("--out", type=Path, required=True)
    args = ap.parse_args()
    manifest = load_manifest(args.corpus)
    paths = videos(manifest)[: args.limit]
    report: dict[str, Any] = {"environment": environment(), "corpus": corpus_info(manifest)}
    if args.decode_only:
        report.update(benchmark="B-DEC", device="cpu (ffmpeg)", decode=decode_only(paths),
                      gate_status="no threshold (reported)")
        write(args.out, report)
        return
    tag = uuid.uuid4().hex[:8]
    base = Settings.from_env()
    settings = base.model_copy(update={
        "scylla": base.scylla.model_copy(update={"keyspace": "fingerprinter_bench"}),
        "redis": base.redis.model_copy(update={"prefix": f"fpbench:{tag}:", "stream_prefix": f"fpbench-ev:{tag}:"}),
        "minio": base.minio.model_copy(update={"representations_bucket": f"fp-bench-{tag}-reps",
                                               "targets_bucket": f"fp-bench-{tag}-targets"}),
        "acquire": base.acquire.model_copy(update={"ssrf_allow_cidrs": ["127.0.0.0/8"]}),
        "scratch_dir": ROOT / "var" / f"bench-{tag}",
        "roles": ["encoder", "scheduler"],
    })
    svc = Services.connect(settings, role="bench", migrate_schema=True)
    wipe(svc.db)
    svc.objects.ensure_buckets()
    svc.pub.seed()
    fp = RuntimeFingerprint.capture(args.device)
    svc.store.put_profile(RuntimeProfile.from_fingerprints(RUNTIME_PROFILE, fp), actor="bench")
    if args.device == "cuda":
        svc.store.admit_variant(RUNTIME_PROFILE, "cuda", actor="bench", evidence="golden check at start")
    spec = dinov2_seg_v2()
    svc.store.add_spec(spec, status="active", actor="bench", origin_profile=RUNTIME_PROFILE)
    svc.store.approve_compat(str(spec.digest), RUNTIME_PROFILE, actor="bench", evidence="origin")
    svc.store.set_role(str(spec.digest), actor="bench")
    for name in (READY, FAILED):
        svc.coord.client.xgroup_create(svc.coord.stream(name), "media-registry", id="0", mkstream=True)
    root = Path(manifest["_dir"])
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), partial(_Quiet, directory=str(root)))
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    url = f"http://127.0.0.1:{httpd.server_address[1]}"
    minutes = 0.0
    sha = {i["file"]: i["sha256"] for i in manifest["items"]}
    for p in paths:
        minutes += (probe(p).duration_s or 0) / 60
        svc.requests.upsert_event(request_env("sha256:" + sha[p.name], f"{url}/{p.name}"))
    enc = Encoder(svc, device=args.device, fingerprint=fp)
    t0 = time.perf_counter()
    code = enc.run(idle_exit_s=5)
    wall = time.perf_counter() - t0 - 5  # minus the idle tail
    reps = svc.db.rows("SELECT segment_count FROM {ks}.representations")
    segs = sum(r["segment_count"] for r in reps)
    stage = {}
    for metric in REGISTRY.collect():
        if metric.name == "fingerprinter_encode_stage_seconds":
            for s in metric.samples:
                if s.name.endswith("_sum"):
                    stage[s.labels["stage"]] = round(s.value, 2)
    report.update({
        "benchmark": "B-GPU-b" if args.device == "cuda" else "B-CPU",
        "device": enc.device, "exit_code": code, "batch": enc.batch,
        "items": len(paths), "ready": len(reps), "video_minutes": round(minutes, 2),
        "wall_s_including_model_load": round(wall, 1),
        "summary": {"s_per_minute_of_video": round(wall / minutes, 3) if minutes else None,
                    "segments_per_s": round(segs / wall, 3) if wall else None},
        "stage_seconds_sum": stage,
        "peak_rss_mib": resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // 1024,
        "vram": {k: v // 2**20 for k, v in (enc.embedders["float32"].vram() or {}).items()},
        "note": "wall time includes model load and golden check (cold start); no end-to-end threshold (P9-D17)",
        # G-6 is the CPU real-media baseline; a CUDA run is B-GPU-b, reported without a gate
        "gate_status": "no threshold (reported); G-6 real-media baseline needs C-PREC"
        if report["corpus"]["name"] != "C-PREC"
        else "measured (G-6 baseline)" if enc.device == "cpu" else "reported (B-GPU-b, no threshold)",
    })
    write(args.out, report)
    httpd.shutdown()
    json.dumps(report, default=str)


if __name__ == "__main__":
    main()
