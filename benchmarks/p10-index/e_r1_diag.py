"""E-R1 diagnosis (validation §13.20): *which* reads make a query slow on the dev host.

E-R1 (§13.5) failed on query latency with the cause inferred as disk-bound
rescoring. This attributes it by measurement on the smallest workload that
reproduces the regime: **one copy** (the 104,443 real cached vectors as
target-role points, §13.3 collection configuration, the E-R1 container
limits: 2 CPUs, no swap) on a dedicated container ``qdrant-e-r1d`` with a
fresh volume. The build runs under the E-R1 cap (1.5 GiB); afterwards the
cap is lowered with ``docker update`` (``--caps``) so that the page cache
can no longer hold the on-disk float32 originals and payload — the state the
6-copy E-R1 run was in at 1.5 GiB (≈ 1.9 GB of originals vs ≈ 0.8 GiB of
cache). At each cap, for each mode (rescore on/off × payload on/off), N
sequential single queries (limit 100, oversampling 2, the 7 films'
segments) after a warm-up; per mode: latency p50/p95/p99 and the host
block-device read operations and sectors per query (``/proc/diskstats`` of
the device holding the Docker root), plus the container's memory.

Not an acceptance run: the E-R1 rules (§10) are unchanged and are judged
only by ``capacity_e_r1.py``. Run alone on the host.

    ~/anti_piracy/datasets/p10/venv/bin/python -I benchmarks/p10-index/e_r1_diag.py \
        --vectors ~/anti_piracy/datasets/p10/vectors --device sda --caps 1536m,640m \
        --out benchmarks/p10-index/results/e-r1-diag.json
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import subprocess
import time
import uuid
from pathlib import Path

import numpy as np

NS = uuid.uuid5(uuid.NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")
IMAGE = "qdrant/qdrant@sha256:d122138f76868edba68d36cb0833139c1d1761f00f09e48e61f8314196e6a4c6"


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout.strip()


def diskstats(dev: str) -> tuple[int, int]:
    for line in Path("/proc/diskstats").read_text().splitlines():
        f = line.split()
        if f[2] == dev:
            return int(f[3]), int(f[5])  # reads completed, sectors read
    raise SystemExit(f"device {dev} not in /proc/diskstats")


def cgroup_of(container: str) -> Path:
    cid = sh("docker", "inspect", "-f", "{{.Id}}", container)
    return Path(f"/sys/fs/cgroup/system.slice/docker-{cid}.scope")


def mem(cg: Path) -> dict:
    stat = dict(line.split() for line in (cg / "memory.stat").read_text().splitlines())
    return {"current": int((cg / "memory.current").read_text()), "anon": int(stat["anon"]), "file": int(stat["file"]),
            "max": (cg / "memory.max").read_text().strip(),
            "oom_kill": int(dict(line.split() for line in (cg / "memory.events").read_text().splitlines())["oom_kill"])}


def main() -> None:  # noqa: PLR0915
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--container", default="qdrant-e-r1d")
    ap.add_argument("--http-port", type=int, default=26343)
    ap.add_argument("--grpc-port", type=int, default=26344)
    ap.add_argument("--device", default="sda")
    ap.add_argument("--caps", default="1536m,640m")
    ap.add_argument("--queries", type=int, default=300)
    ap.add_argument("--warmup", type=int, default=50)
    ap.add_argument("--keep", action="store_true", help="leave the container and volume (default: remove both)")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    from qdrant_client import QdrantClient, models

    key = uuid.uuid4().hex
    vol = f"{a.container}-data"
    if a.container in sh("docker", "ps", "-a", "--format", "{{.Names}}").split():
        raise SystemExit(f"refusing: container {a.container} exists")
    sh("docker", "volume", "create", vol)
    sh("docker", "run", "-d", "--name", a.container, "--memory", "1536m", "--memory-swap", "1536m", "--cpus", "2",
       "-p", f"127.0.0.1:{a.http_port}:6333", "-p", f"127.0.0.1:{a.grpc_port}:6334",
       "-e", f"QDRANT__SERVICE__API_KEY={key}", "-e", "QDRANT__TELEMETRY_DISABLED=true",
       "-v", f"{vol}:/qdrant/storage", IMAGE)
    report: dict = {"benchmark": "P10-E-R1-DIAG", "container": a.container, "image": IMAGE, "cpus": 2,
                    "device": a.device, "host": {"kernel": platform.release(), "nproc": os.cpu_count()},
                    "device_info": sh("lsblk", "-d", "-o", "NAME,ROTA,MODEL,TRAN", f"/dev/{a.device}"), "caps": {}}
    try:
        client = QdrantClient(host="127.0.0.1", port=a.http_port, grpc_port=a.grpc_port, prefer_grpc=True,
                              api_key=key, https=False, timeout=600)
        for _ in range(60):
            try:
                report["qdrant_version"] = client.info().version
                break
            except Exception:  # noqa: BLE001 - starting
                time.sleep(1)
        cg = cgroup_of(a.container)
        base, queries = [], []
        for sub in ("soak-1000", "l-pairs"):
            for f in sorted((a.vectors / sub).glob("*.npz")):
                z = np.load(f)
                (queries if f.name.startswith("targets__") else base).append(z["vectors"].astype(np.float32))
        X, Q = np.vstack(base), np.vstack(queries)
        m = models
        client.create_collection("g1", vectors_config=m.VectorParams(size=768, distance=m.Distance.DOT, on_disk=True),
                                 hnsw_config=m.HnswConfigDiff(m=32, ef_construct=200), on_disk_payload=True,
                                 quantization_config=m.ScalarQuantization(scalar=m.ScalarQuantizationConfig(
                                     type=m.ScalarType.INT8, always_ram=True)))
        for fname in ("rep", "trep", "cid", "tid", "spec"):
            client.create_payload_index("g1", fname, m.PayloadSchemaType.KEYWORD)
        client.create_payload_index("g1", "seq", m.PayloadSchemaType.INTEGER)
        t0 = time.time()
        for i in range(0, len(X), 1000):
            pts = [m.PointStruct(id=str(uuid.uuid5(NS, f"t{j // 100}:1:{j % 100}")), vector=X[j].tolist(),
                                 payload={"rep": f"r{j // 100}", "trep": f"t{j // 100}", "cid": f"c{j // 100}",
                                          "tid": f"tid{j // 700}", "tver": 1, "spec": "e-r1", "seq": 1,
                                          "k": j % 100, "t": 5.0 * (j % 100)})
                   for j in range(i, min(i + 1000, len(X)))]
            client.upsert("g1", pts, wait=True)
        while client.get_collection("g1").status != m.CollectionStatus.GREEN:
            time.sleep(1)
        report["points"] = client.count("g1", exact=True).count
        report["build_seconds"] = round(time.time() - t0, 1)
        report["volume_bytes"] = int(sh("docker", "exec", a.container, "du", "-sb", "/qdrant/storage").split()[0])
        rng = np.random.default_rng(20)
        order = rng.integers(len(Q), size=a.warmup + a.queries)
        for cap in a.caps.split(","):
            sh("docker", "update", "--memory", cap, "--memory-swap", cap, a.container)
            time.sleep(5)
            out: dict = {"memory_before": mem(cg), "modes": {}}
            for rescore in (True, False):
                for payload in (True, False):
                    params = m.SearchParams(quantization=m.QuantizationSearchParams(rescore=rescore, oversampling=2.0))
                    lat, ops = [], []
                    for n, qi in enumerate(order):
                        r0 = diskstats(a.device)
                        t = time.perf_counter()
                        client.query_points("g1", query=Q[qi].tolist(), limit=100, search_params=params,
                                            with_payload=payload)
                        dt = time.perf_counter() - t
                        r1 = diskstats(a.device)
                        if n >= a.warmup:
                            lat.append(1000 * dt)
                            ops.append((r1[0] - r0[0], r1[1] - r0[1]))
                    o = np.array(ops)
                    key_ = f"rescore={rescore},payload={payload}"
                    out["modes"][key_] = {"p50_ms": round(float(np.median(lat)), 2),
                                          "p95_ms": round(float(np.percentile(lat, 95)), 2),
                                          "p99_ms": round(float(np.percentile(lat, 99)), 2),
                                          "disk_reads_per_query": round(float(o[:, 0].mean()), 1),
                                          "kib_read_per_query": round(float(o[:, 1].mean()) / 2, 1),
                                          "queries": len(lat)}
                    print(cap, key_, out["modes"][key_], flush=True)
            out["memory_after"] = mem(cg)
            report["caps"][cap] = out
    finally:
        if not a.keep:
            subprocess.run(["docker", "rm", "-f", a.container], check=False, capture_output=True)
            subprocess.run(["docker", "volume", "rm", vol], check=False, capture_output=True)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))


if __name__ == "__main__":
    main()
