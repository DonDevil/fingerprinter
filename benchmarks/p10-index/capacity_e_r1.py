"""E-R1 — capacity boundary of one 1.5 GiB Qdrant 1.15.1 instance (design §13.4, validation §10).

Question: does one hard-1.5 GiB instance hold an N-point target-role
generation **and** its rebuild (two generations resident, live writes,
queries)? Run on a dedicated, separately named container and volume
(``qdrant-e-r1``), never on the dev instance; the production capacity guard
is not involved (it takes its limit from this measurement).

Corpus: the 104,443 cached real ``dinov2-seg/2`` vectors (SOAK-1000 run 5 +
L-PAIRS clips) and ``copies - 1`` seeded random orthogonal rotations of
them (norms and intra-copy geometry kept; valid for memory/latency, not for
recall), target-role payload, the §13.3 collection configuration, gRPC
batches of 1,000. A 10,000-point media-role collection stays resident in
every phase (C-12). Resident points are counted exactly (``count exact``
per collection at every phase boundary; the harness's running total in
between). Test ceiling ``P_test`` = 1,330,000.

Phases: (1) build g1 (N target points) + media 10,000, wait green;
(2) warm queries 3 x 760 (the 7 films' segments); (3) build g2 of the same
set while g1 serves 10 queries/s (concurrency 2, limit 100, rescoring) and a
12-segment live write per second goes to every routed generation (9 of 10
new subjects, every 10th a re-UPSERT at a new ``seq`` followed by its
``seq <`` cleanup); after the build, continue until resident ≥
``R_req(N) = (N + 10,000) + ⌈1.05 N⌉ + 10,000``; (4) promote (harness
pointer), delete g1; (5) 5 min steady queries.

Pass (all): no ``oom_kill``; cgroup ``anon`` peak ≤ 1.35 GiB; query p95 ≤
10 ms in phases 2, 3 and 5 after a 60 s warm-up; exact counts at every phase
boundary equal the harness's resident total.

    ~/anti_piracy/datasets/p10/venv/bin/python -I benchmarks/p10-index/capacity_e_r1.py \
        --vectors ~/anti_piracy/datasets/p10/vectors --copies 6 --grpc-port 26334 --http-port 26333 \
        --container qdrant-e-r1 --out benchmarks/p10-index/results/e-r1-6copies.json
"""

from __future__ import annotations

import argparse
import json
import os
import math
import platform
import subprocess
import threading
import time
import uuid
from pathlib import Path

import numpy as np

GiB = 1024**3
NS = uuid.uuid5(uuid.NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")
P_TEST = 1_330_000
MEDIA_RESIDENT = 10_000
MARGIN = 10_000
ANON_LIMIT = int(1.35 * GiB)


def pid(subject: str, seq: int, k: int) -> str:
    return str(uuid.uuid5(NS, f"{subject}:{seq}:{k}"))


def r_req(n: int) -> int:
    return (n + MEDIA_RESIDENT) + math.ceil(1.05 * n) + MARGIN


def load_corpus(vectors: Path) -> tuple[np.ndarray, np.ndarray]:
    base, queries = [], []
    for sub in ("soak-1000", "l-pairs"):
        for f in sorted((vectors / sub).glob("*.npz")):
            z = np.load(f)
            (queries if f.name.startswith("targets__") else base).append(z["vectors"].astype(np.float32))
    return np.vstack(base), np.vstack(queries)


def rotation(dim: int, seed: int) -> np.ndarray:
    rng = np.random.default_rng(seed)
    q, r = np.linalg.qr(rng.standard_normal((dim, dim)))
    return (q * np.sign(np.diag(r))).astype(np.float32)


class Sampler(threading.Thread):
    """cgroup memory every second (memory.current, anon, file, peak, oom_kill) plus phase labels."""

    def __init__(self, cgroup: Path) -> None:
        super().__init__(daemon=True)
        self.cg = cgroup
        self.phase = "init"
        self.samples: list[dict] = []
        self.stop = threading.Event()

    def read(self) -> dict:
        stat = dict(line.split() for line in (self.cg / "memory.stat").read_text().splitlines())
        events = dict(line.split() for line in (self.cg / "memory.events").read_text().splitlines())
        return {"t": time.time(), "phase": self.phase, "current": int((self.cg / "memory.current").read_text()),
                "anon": int(stat["anon"]), "file": int(stat["file"]),
                "peak": int((self.cg / "memory.peak").read_text()), "oom_kill": int(events["oom_kill"])}

    def run(self) -> None:
        while not self.stop.is_set():
            try:
                self.samples.append(self.read())
            except OSError:
                self.samples.append({"t": time.time(), "phase": self.phase, "error": "cgroup gone"})
            self.stop.wait(1.0)


class Harness:
    def __init__(self, a: argparse.Namespace) -> None:
        from qdrant_client import QdrantClient, models

        self.m = models
        self.a = a
        key = a.api_key
        self.client = QdrantClient(host="127.0.0.1", port=a.http_port, grpc_port=a.grpc_port, prefer_grpc=True,
                                   api_key=key, https=False, timeout=600)
        self.resident: dict[str, int] = {}
        self.lock = threading.Lock()
        self.events: list[dict] = []
        self.query_lat: list[tuple[str, float, float]] = []   # (phase, t, seconds)
        self.write_lat: list[tuple[str, float, float]] = []
        self.errors: list[str] = []

    def log(self, **kw) -> None:
        kw["t"] = time.time()
        self.events.append(kw)
        print(json.dumps(kw, default=str), flush=True)

    def create(self, name: str, role: str) -> None:
        m = self.m
        self.client.create_collection(
            name,
            vectors_config=m.VectorParams(size=768, distance=m.Distance.DOT, on_disk=True),
            hnsw_config=m.HnswConfigDiff(m=32, ef_construct=200),
            quantization_config=m.ScalarQuantization(scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8,
                                                                                         always_ram=True)),
            on_disk_payload=True)
        fields = ("rep", "cid", "spec") if role == "media" else ("rep", "trep", "cid", "tid", "spec")
        for f in fields:
            self.client.create_payload_index(name, f, m.PayloadSchemaType.KEYWORD, wait=True)
        self.client.create_payload_index(name, "seq", m.PayloadSchemaType.INTEGER, wait=True)
        self.resident[name] = 0

    def payload(self, role: str, subject: str, seq: int, k: int) -> dict:
        p = {"rep": "rep:" + subject, "cid": "cnt:" + subject, "spec": "sha256:e-r1", "k": k, "t": 5.0 * k, "seq": seq}
        if role == "target":
            p.update({"trep": subject, "tid": "tgt:" + subject, "tver": 1, "lowinfo": False, "lowinfo_rev": 1})
        return p

    def upsert(self, name: str, role: str, rows: list[tuple[str, int, int, np.ndarray]]) -> None:
        pts = [self.m.PointStruct(id=pid(s, q, k), vector=v.tolist(), payload=self.payload(role, s, q, k))
               for s, q, k, v in rows]
        self.client.upsert(name, pts, wait=True)
        with self.lock:
            self.resident[name] += len(pts)

    def bulk(self, name: str, role: str, X: np.ndarray, seg_per_subject: int = 12) -> float:
        t0 = time.time()
        batch = []
        for i in range(len(X)):
            batch.append((f"s{i // seg_per_subject}", 1, i % seg_per_subject, X[i]))
            if len(batch) == 1000:
                self.upsert(name, role, batch)
                batch = []
        if batch:
            self.upsert(name, role, batch)
        return time.time() - t0

    def wait_green(self, name: str, timeout: float = 7200) -> float:
        t0 = time.time()
        while time.time() - t0 < timeout:
            info = self.client.get_collection(name)
            if str(info.status).lower().endswith("green"):
                return time.time() - t0
            time.sleep(2)
        raise TimeoutError(f"{name} not green after {timeout}s")

    def exact_total(self) -> dict[str, int]:
        return {n: self.client.count(n, exact=True).count for n in self.resident}

    def boundary(self, phase: str) -> dict:
        exact = self.exact_total()
        ok = exact == self.resident
        self.log(boundary=phase, exact=exact, harness=dict(self.resident), equal=ok)
        return {"exact": exact, "harness": dict(self.resident), "equal": ok, "total": sum(exact.values())}

    # --- workloads -----------------------------------------------------------------

    def queries(self, stop: threading.Event, phase_of, names_of, Q: np.ndarray, rate: float, seed: int) -> None:
        rng = np.random.default_rng(seed)
        params = self.m.SearchParams(quantization=self.m.QuantizationSearchParams(rescore=True, oversampling=2.0))
        period = 1.0 / rate
        nxt = time.time()
        while not stop.is_set():
            q = Q[int(rng.integers(len(Q)))]
            t0 = time.time()
            try:
                self.client.query_points(names_of(), query=q.tolist(), limit=100, search_params=params)
                self.query_lat.append((phase_of(), t0, time.time() - t0))
            except Exception as exc:  # noqa: BLE001 - recorded
                self.errors.append(f"query: {type(exc).__name__}: {exc}"[:300])
            nxt += period
            time.sleep(max(0.0, nxt - time.time()))

    def live(self, stop: threading.Event, phase_of, routed, Xlive: np.ndarray, ceiling: int) -> None:
        n, subj_i, used = 0, 0, 0
        existing: list[tuple[str, int]] = []
        nxt = time.time()
        while not stop.is_set():
            n += 1
            gens = routed()
            if n % 10 == 0 and existing:  # re-UPSERT of an existing subject at a new seq, then cleanup
                s, seq = existing[(n // 10) % len(existing)]
                new = seq + 1
                rows = [(s, new, k, Xlive[(used + k) % len(Xlive)]) for k in range(12)]
            else:
                if sum(self.resident.values()) + 12 * len(gens) > ceiling:
                    self.log(live="ceiling reached; live writes stopped", resident=sum(self.resident.values()))
                    return
                s, new = f"live{subj_i}", 1
                subj_i += 1
                rows = [(s, new, k, Xlive[(used + k) % len(Xlive)]) for k in range(12)]
            used += 12
            for g in gens:
                t0 = time.time()
                try:
                    self.upsert(g, "target", rows)
                    if new > 1:
                        flt = self.m.Filter(must=[
                            self.m.FieldCondition(key="trep", match=self.m.MatchValue(value=s)),
                            self.m.FieldCondition(key="seq", range=self.m.Range(lt=new))])
                        before = self.client.count(g, count_filter=flt, exact=True).count
                        self.client.delete(g, points_selector=self.m.FilterSelector(filter=flt), wait=True)
                        with self.lock:
                            self.resident[g] -= before
                    self.write_lat.append((phase_of(), t0, time.time() - t0))
                except Exception as exc:  # noqa: BLE001 - recorded
                    self.errors.append(f"live: {type(exc).__name__}: {exc}"[:300])
            if new == 1:
                existing.append((s, 1))
            else:
                existing[(n // 10) % len(existing)] = (s, new)
            nxt += 1.0
            time.sleep(max(0.0, nxt - time.time()))


def pct(xs: list[float], p: float) -> float | None:
    return round(float(np.percentile(xs, p)) * 1000, 3) if xs else None


def du(container: str) -> int | None:
    try:
        out = subprocess.run(["sg", "docker", "-c", f"docker exec {container} du -sb /qdrant/storage"],
                             capture_output=True, text=True, timeout=120).stdout.split()
        return int(out[0]) if out else None
    except (OSError, ValueError, subprocess.TimeoutExpired):
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--copies", type=int, default=6)
    ap.add_argument("--http-port", type=int, default=26333)
    ap.add_argument("--grpc-port", type=int, default=26334)
    ap.add_argument("--api-key", default=os.environ.get("FINGERPRINTER_INDEX__API_KEY"))
    ap.add_argument("--container", default="qdrant-e-r1")
    ap.add_argument("--cgroup", type=Path, required=True)
    ap.add_argument("--steady-s", type=float, default=300.0)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--samples", type=Path, required=True, help="per-second samples (JSONL, outside git)")
    ap.add_argument("--subset", type=int, help="first N base vectors only: harness check, never a gate")
    a = ap.parse_args()

    base, Q = load_corpus(a.vectors)
    if a.subset:
        base = base[: a.subset]
    # one preallocated array (the copies are built in place: the corpus is held once, ~1.9 GB at 6 copies)
    X = np.empty((len(base) * a.copies, base.shape[1]), dtype=np.float32)
    X[: len(base)] = base
    for c in range(1, a.copies):
        np.matmul(base, rotation(768, 1000 + c), out=X[c * len(base):(c + 1) * len(base)])
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    N = len(X)
    Xlive = (base @ rotation(768, 999)).astype(np.float32)
    Xmedia = (np.vstack([base] * math.ceil(MEDIA_RESIDENT / len(base)))[:MEDIA_RESIDENT]
              @ rotation(768, 998)).astype(np.float32)
    req = r_req(N)
    h = Harness(a)
    sampler = Sampler(a.cgroup)
    sampler.start()
    report: dict = {"benchmark": "E-R1", "gate_eligible": not a.subset, "copies": a.copies, "N": N,
                    "base_vectors": len(base), "R_req": req,
                    "P_test": P_TEST, "queries": len(Q), "qdrant_version": None, "phases": {}}
    phase = {"name": "1-build-g1"}
    routed: list[str] = []
    phase_of = lambda: phase["name"]  # noqa: E731
    try:
        sampler.phase = phase["name"]
        h.create("media", "media")
        h.bulk("media", "media", Xmedia)
        h.create("g1", "target")
        t_build = h.bulk("g1", "target", X)
        t_green = h.wait_green("g1")
        h.wait_green("media")
        report["phases"]["1"] = {"ingest_s": round(t_build, 1), "index_green_s": round(t_green, 1),
                                 "boundary": h.boundary("1"), "du_bytes": du(a.container)}

        phase["name"] = sampler.phase = "2-warm"
        t0 = time.time()
        params = h.m.SearchParams(quantization=h.m.QuantizationSearchParams(rescore=True, oversampling=2.0))
        for _ in range(3):
            for q in Q:
                s = time.time()
                h.client.query_points("g1", query=q.tolist(), limit=100, search_params=params)
                h.query_lat.append(("2-warm", s, time.time() - s))
        report["phases"]["2"] = {"seconds": round(time.time() - t0, 1), "boundary": h.boundary("2")}

        phase["name"] = sampler.phase = "3-rebuild"
        h.create("g2", "target")
        routed[:] = ["g1", "g2"]
        stop = threading.Event()
        workers = [threading.Thread(target=h.queries, args=(stop, phase_of, lambda: "g1", Q, 5.0, s), daemon=True)
                   for s in (1, 2)]
        workers.append(threading.Thread(target=h.live, args=(stop, phase_of, lambda: list(routed), Xlive, P_TEST),
                                        daemon=True))
        for w in workers:
            w.start()
        t0 = time.time()
        t_build2 = h.bulk("g2", "target", X)
        t_green2 = h.wait_green("g2")
        h.log(phase3="g2 built", ingest_s=round(t_build2, 1), green_s=round(t_green2, 1),
              resident=sum(h.resident.values()))
        while sum(h.resident.values()) < req:
            if not workers[-1].is_alive():
                break
            time.sleep(5)
        reached = sum(h.resident.values())
        stop.set()
        for w in workers:
            w.join(timeout=60)
        report["phases"]["3"] = {"seconds": round(time.time() - t0, 1), "g2_ingest_s": round(t_build2, 1),
                                 "g2_green_s": round(t_green2, 1), "resident_reached": reached,
                                 "reached_R_req": reached >= req, "boundary": h.boundary("3"),
                                 "du_bytes": du(a.container)}

        phase["name"] = sampler.phase = "4-promote"
        routed[:] = ["g2"]
        h.client.delete_collection("g1")
        del h.resident["g1"]
        report["phases"]["4"] = {"boundary": h.boundary("4"), "du_bytes": du(a.container)}

        phase["name"] = sampler.phase = "5-steady"
        stop = threading.Event()
        workers = [threading.Thread(target=h.queries, args=(stop, phase_of, lambda: "g2", Q, 5.0, s), daemon=True)
                   for s in (3, 4)]
        for w in workers:
            w.start()
        time.sleep(a.steady_s)
        stop.set()
        for w in workers:
            w.join(timeout=60)
        report["phases"]["5"] = {"seconds": a.steady_s, "boundary": h.boundary("5")}
        report["qdrant_version"] = h.client.info().version
    except Exception as exc:  # noqa: BLE001 - a failure is a result
        report["failure"] = {"phase": phase["name"], "error": f"{type(exc).__name__}: {exc}"[:500]}
        h.log(failure=report["failure"])
    finally:
        time.sleep(2)
        sampler.stop.set()
        sampler.join(timeout=5)

    samples = [s for s in sampler.samples if "anon" in s]
    by_phase: dict = {}
    for s in samples:
        d = by_phase.setdefault(s["phase"], {"anon_peak": 0, "current_peak": 0, "file_peak": 0})
        d["anon_peak"] = max(d["anon_peak"], s["anon"])
        d["current_peak"] = max(d["current_peak"], s["current"])
        d["file_peak"] = max(d["file_peak"], s["file"])
    oom = max((s["oom_kill"] for s in samples), default=0)
    anon_peak = max((s["anon"] for s in samples), default=0)
    lat = {}
    for ph in ("2-warm", "3-rebuild", "5-steady"):
        start = min((t for p, t, _ in h.query_lat if p == ph), default=None)
        xs = [d for p, t, d in h.query_lat if p == ph and start is not None and t >= start + 60]
        lat[ph] = {"n_after_warmup": len(xs), "p50_ms": pct(xs, 50), "p95_ms": pct(xs, 95), "p99_ms": pct(xs, 99)}
    wl = [d for _, _, d in h.write_lat]
    boundaries_equal = all(p.get("boundary", {}).get("equal", False) for p in report["phases"].values())
    p95_ok = all((lat[ph]["p95_ms"] is not None and lat[ph]["p95_ms"] <= 10.0) for ph in lat)
    report.update({
        "memory": {"by_phase": by_phase, "anon_peak_bytes": anon_peak, "anon_peak_gib": round(anon_peak / GiB, 4),
                   "memory_peak_bytes": max((s["peak"] for s in samples), default=0), "oom_kill": oom,
                   "limit_bytes": int((a.cgroup / "memory.max").read_text())
                   if (a.cgroup / "memory.max").exists() else None},
        "query_latency": lat,
        "live_write_latency_ms": {"n": len(wl), "p50": pct(wl, 50), "p95": pct(wl, 95), "p99": pct(wl, 99)},
        "errors": h.errors[:50], "error_count": len(h.errors),
        "pass_rules": {"no_oom_kill": oom == 0, "anon_peak_le_1_35_gib": anon_peak <= ANON_LIMIT,
                       "query_p95_le_10ms_phases_2_3_5": p95_ok, "exact_counts_equal_at_boundaries": boundaries_equal,
                       "phase3_reached_R_req": bool(report["phases"].get("3", {}).get("reached_R_req")),
                       "completed": "failure" not in report},
        "host": platform.node(), "samples": len(samples),
    })
    report["pass"] = all(report["pass_rules"].values())
    report["measured_max_resident_total"] = max((p.get("boundary", {}).get("total", 0)
                                                 for p in report["phases"].values()), default=0)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    a.samples.parent.mkdir(parents=True, exist_ok=True)
    a.samples.write_text("\n".join(json.dumps(s) for s in sampler.samples))
    report["samples_file"] = str(a.samples)
    print(json.dumps({k: report[k] for k in ("pass", "pass_rules", "memory", "query_latency")}, indent=1, default=str))


if __name__ == "__main__":
    main()
