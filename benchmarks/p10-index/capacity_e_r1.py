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
import math
import os
import platform
import subprocess
import sys
import tempfile
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
        self.map_bytes = None   # callable returning the bytes held by the subject maps (FP-05 A)

    def read(self) -> dict:
        stat = dict(line.split() for line in (self.cg / "memory.stat").read_text().splitlines())
        events = dict(line.split() for line in (self.cg / "memory.events").read_text().splitlines())
        out = {"t": time.time(), "phase": self.phase, "current": int((self.cg / "memory.current").read_text()),
               "anon": int(stat["anon"]), "file": int(stat["file"]),
               "peak": int((self.cg / "memory.peak").read_text()), "oom_kill": int(events["oom_kill"])}
        out.update(self.extra())
        return out

    def extra(self) -> dict:
        """FP-05 A: CPU and disk I/O of the Qdrant cgroup, and memory/CPU of this (client + sidecar) process."""
        out: dict = {}
        try:
            cpu = dict(line.split() for line in (self.cg / "cpu.stat").read_text().splitlines())
            out["cg_cpu_usec"] = int(cpu["usage_usec"])
            out["cg_cpu_throttled_usec"] = int(cpu.get("throttled_usec", 0))
            rb = wb = ri = wi = 0
            for line in (self.cg / "io.stat").read_text().splitlines():
                kv = dict(x.split("=") for x in line.split()[1:])
                rb += int(kv.get("rbytes", 0)); wb += int(kv.get("wbytes", 0))
                ri += int(kv.get("rios", 0)); wi += int(kv.get("wios", 0))
            out.update({"cg_io_rbytes": rb, "cg_io_wbytes": wb, "cg_io_rios": ri, "cg_io_wios": wi})
        except (OSError, ValueError, KeyError):
            pass
        try:
            st = {k: v for k, v in (ln.split(":", 1) for ln in open("/proc/self/status") if ":" in ln)}
            out["proc_rss_kb"] = int(st["VmRSS"].split()[0])
            out["proc_anon_kb"] = int(st["RssAnon"].split()[0])
            f = open("/proc/self/stat").read().rsplit(")", 1)[1].split()
            out["proc_cpu_ticks"] = int(f[11]) + int(f[12])          # utime + stime (clock ticks)
            mem = {k: v for k, v in (ln.split(":", 1) for ln in open("/proc/meminfo"))}
            out["host_mem_available_kb"] = int(mem["MemAvailable"].split()[0])
            if self.map_bytes is not None:
                out["map_bytes"] = self.map_bytes()
        except (OSError, ValueError, KeyError):
            pass
        return out

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
        # FP-05: ``--payload-fields`` limits the payload keys returned by every query; unset = the original
        # methodology (qdrant-client default, whole payload). Stored payloads and all thresholds are unchanged.
        self.payload_kw: dict = {}
        # FP-05 design trial: collection layout overrides (``--indexing-threads``); default = original §13.3 config.
        self.layout_kw: dict = {}
        # FP-05 Investigation A: ``--subject-map memory`` = Qdrant returns ids+scores only (no payload) and the
        # metadata is resolved from a sidecar SubjectMap kept per collection (generation) in this process.
        self.maps: dict = {}
        self.sm_cls = None
        self.lookup_lat: list[tuple[str, float, float]] = []   # (phase, t, seconds) of the map resolution alone
        self.map_events: dict = {}
        self.score_threshold = None

    def log(self, **kw) -> None:
        kw["t"] = time.time()
        self.events.append(kw)
        print(json.dumps(kw, default=str), flush=True)

    def create(self, name: str, role: str) -> None:
        m = self.m
        self.client.create_collection(
            name,
            vectors_config=m.VectorParams(size=768, distance=m.Distance.DOT, on_disk=True),
            hnsw_config=m.HnswConfigDiff(m=32, ef_construct=200, **({"max_indexing_threads": self.a.indexing_threads} if self.a.indexing_threads else {})),
            quantization_config=m.ScalarQuantization(scalar=m.ScalarQuantizationConfig(type=m.ScalarType.INT8,
                                                                                         always_ram=True)),
            on_disk_payload=True, **self.layout_kw)
        fields = ("rep", "cid", "spec") if role == "media" else ("rep", "trep", "cid", "tid", "spec")
        for f in fields:
            self.client.create_payload_index(name, f, m.PayloadSchemaType.KEYWORD, wait=True)
        self.client.create_payload_index(name, "seq", m.PayloadSchemaType.INTEGER, wait=True)
        self.resident[name] = 0
        if self.sm_cls is not None:
            self.maps[name] = self.sm_cls(name, spec="sha256:e-r1", role=role, generation=int(name[1:]) if name[0] == "g" else 0)

    def payload(self, role: str, subject: str, seq: int, k: int) -> dict:
        p = {"rep": "rep:" + subject, "cid": "cnt:" + subject, "spec": "sha256:e-r1", "k": k, "t": 5.0 * k, "seq": seq}
        if role == "target":
            p.update({"trep": subject, "tid": "tgt:" + subject, "tver": 1, "lowinfo": False, "lowinfo_rev": 1})
        return p

    def upsert(self, name: str, role: str, rows: list[tuple[str, int, int, np.ndarray]]) -> None:
        pts = [self.m.PointStruct(id=pid(s, q, k), vector=v.tolist(), payload=self.payload(role, s, q, k))
               for s, q, k, v in rows]
        sm = self.maps.get(name)
        if sm is not None:   # write-ahead, exactly as Indexer._apply_to does: the map is a superset of Qdrant
            sm.record_many(((p.id, p.payload) for p in pts), "trep" if role == "target" else "rep")
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
        out = {"exact": exact, "harness": dict(self.resident), "equal": ok, "total": sum(exact.values())}
        if self.maps:   # FP-05 A: the map of every resident collection holds exactly Qdrant's exact count
            mc = {n: len(sm) for n, sm in self.maps.items() if n in exact}
            out["map_counts"] = mc
            out["map_equal"] = all(mc.get(n) == c for n, c in exact.items() if n in self.maps)
            ok = ok and out["map_equal"]
            out["equal"] = ok
        self.log(boundary=phase, **out)
        return out

    def search(self, name: str, q, params, phase: str = ""):
        """One candidate search as the matcher issues it: payload from Qdrant (baseline) or from the map."""
        kw = dict(self.payload_kw)
        sm = self.maps.get(name)
        if sm is None:
            return self.client.query_points(name, query=q.tolist(), limit=100, search_params=params, **kw)
        kw["with_payload"] = False
        res = self.client.query_points(name, query=q.tolist(), limit=100, search_params=params, **kw)
        t = time.perf_counter()
        metas = sm.get([p.id for p in res.points])
        dt = time.perf_counter() - t
        self.lookup_lat.append((phase, time.time(), dt))
        if any(m is None for m in metas):
            self.errors.append("map miss during a query")
        return res, metas

    def equality_check(self, name: str, Q) -> dict:
        """Candidate identity: for every query, ids, scores and metadata of the map path equal the Qdrant-payload path."""
        base_kw = dict(self.payload_kw)
        sm = self.maps.get(name)
        params = self.m.SearchParams(quantization=self.m.QuantizationSearchParams(rescore=True, oversampling=2.0))
        bad = 0
        hits = 0
        for q in Q:
            a = self.client.query_points(name, query=q.tolist(), limit=100, search_params=params, **base_kw)
            b = self.client.query_points(name, query=q.tolist(), limit=100, search_params=params,
                                         **{**base_kw, "with_payload": False})
            metas = sm.get([p.id for p in b.points])
            hits += len(a.points)
            if [(p.id, p.score) for p in a.points] != [(p.id, p.score) for p in b.points]:
                bad += 1
                continue
            keep = ("rep", "cid", "trep", "tid", "tver", "seq", "k", "spec", "lowinfo")
            if [{k: v for k, v in (p.payload or {}).items() if k in keep} for p in a.points] != metas:
                bad += 1
        out = {"collection": name, "queries": len(Q), "hits": hits, "mismatched_queries": bad}
        self.log(equality=out)
        return out

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
                self.search(names_of(), q, params, phase_of())
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
                        if g in self.maps:
                            self.maps[g].remove_subject(s, seq_lt=new)
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
    ap.add_argument("--payload-fields", help="FP-05 trial: comma-separated payload keys returned by queries "
                                             "(default: whole payload, the original E-R1 methodology)")
    ap.add_argument("--score-threshold", type=float, help="FP-05: pass score_threshold to every query, as the "
                                                          "production candidate search now does (policy floor)")
    ap.add_argument("--indexing-threads", type=int, help="FP-05: hnsw max_indexing_threads and optimizer "
                                                         "max_optimization_threads (default: Qdrant auto)")
    ap.add_argument("--subject-map", choices=["off", "memory"], default="off",
                    help="FP-05 Investigation A: 'memory' = Qdrant returns ids+scores only; the matcher metadata is "
                         "resolved from the in-process sidecar SubjectMap (default off = the Qdrant-payload baseline)")
    ap.add_argument("--map-dir", type=Path, help="where the maps are checkpointed (default: a temp dir beside --out)")
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
    if a.subject_map != "off":
        sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
        from fingerprinter.subjectmap import SubjectMap   # noqa: PLC0415
        h.sm_cls = SubjectMap
        mapdir = a.map_dir or Path(tempfile.mkdtemp(prefix="fp05a-maps-", dir=str(a.out.parent)))
        sampler_map_bytes = lambda: sum(sm.nbytes() for sm in list(h.maps.values()))   # noqa: E731
    if a.payload_fields:
        h.payload_kw = {"with_payload": [f for f in a.payload_fields.split(",") if f]}
    if a.score_threshold is not None:
        h.payload_kw["score_threshold"] = a.score_threshold
    if a.indexing_threads:
        h.layout_kw = {"optimizers_config": h.m.OptimizersConfigDiff(max_optimization_threads=a.indexing_threads)}
    sampler = Sampler(a.cgroup)
    if a.subject_map != "off":
        sampler.map_bytes = sampler_map_bytes
    rss0 = sampler.extra()
    sampler.start()
    report: dict = {"benchmark": "E-R1", "gate_eligible": not a.subset, "copies": a.copies, "N": N,
                    "base_vectors": len(base), "R_req": req,
                    "P_test": P_TEST, "queries": len(Q), "qdrant_version": None, "phases": {},
                    "query_payload": a.payload_fields or "whole payload (original methodology)",
                    "score_threshold": a.score_threshold, "indexing_threads": a.indexing_threads,
                    "subject_map": a.subject_map, "proc_start": {k: v for k, v in rss0.items() if k.startswith("proc_")}}
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
        if h.maps:   # FP-05 A: seal g1's map (checkpoint, then serve the *loaded* copy, as production would)
            sm1 = h.maps["g1"]
            before = sm1.nbytes()
            t = time.time(); sm1.save(mapdir); save_s = time.time() - t
            sm1.close()
            t = time.time()
            h.maps["g1"] = SubjectMap.load(mapdir, "g1", spec="sha256:e-r1", role="target", generation=1)
            load_s = time.time() - t
            h.map_events["g1"] = {"save_s": round(save_s, 3), "load_s": round(load_s, 3),
                                  "disk_bytes": sum(f.stat().st_size for f in (mapdir / "g1").iterdir()),
                                  "entries": len(h.maps["g1"]), "nbytes_before_save": before,
                                  "nbytes_after_load": h.maps["g1"].nbytes()}
            h.log(map_g1=h.map_events["g1"])
        report["phases"]["1"] = {"ingest_s": round(t_build, 1), "index_green_s": round(t_green, 1),
                                 "boundary": h.boundary("1"), "du_bytes": du(a.container)}

        phase["name"] = sampler.phase = "2-warm"
        t0 = time.time()
        params = h.m.SearchParams(quantization=h.m.QuantizationSearchParams(rescore=True, oversampling=2.0))
        for _ in range(3):
            for q in Q:
                s = time.time()
                h.search("g1", q, params, "2-warm")
                h.query_lat.append(("2-warm", s, time.time() - s))
        report["phases"]["2"] = {"seconds": round(time.time() - t0, 1), "boundary": h.boundary("2")}
        if h.maps:
            report["equality_g1"] = h.equality_check("g1", Q)

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
        if h.maps:   # g2 is sealed at the end of the rebuild (map checkpoint, journal on for later live writes)
            t = time.time(); h.maps["g2"].save(mapdir); save2 = time.time() - t
            h.map_events["g2"] = {"save_s": round(save2, 3), "entries": len(h.maps["g2"]),
                                  "disk_bytes": sum(f.stat().st_size for f in (mapdir / "g2").iterdir())}
            h.log(map_g2=h.map_events["g2"])
        report["phases"]["3"] = {"seconds": round(time.time() - t0, 1), "g2_ingest_s": round(t_build2, 1),
                                 "g2_green_s": round(t_green2, 1), "resident_reached": reached,
                                 "reached_R_req": reached >= req, "boundary": h.boundary("3"),
                                 "du_bytes": du(a.container)}

        phase["name"] = sampler.phase = "4-promote"
        routed[:] = ["g2"]
        h.client.delete_collection("g1")
        del h.resident["g1"]
        if "g1" in h.maps:   # the generation is gone: its map and files go with it
            h.maps.pop("g1").close()
            import shutil
            shutil.rmtree(mapdir / "g1", ignore_errors=True)
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
        if h.maps:
            report["equality_g2"] = h.equality_check("g2", Q)
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
    lk = {}
    for ph in ("2-warm", "3-rebuild", "5-steady"):
        xs = [d for p, t, d in h.lookup_lat if p == ph]
        lk[ph] = {"n": len(xs), "p50_us": None if not xs else round(float(np.percentile(xs, 50)) * 1e6, 1),
                  "p95_us": None if not xs else round(float(np.percentile(xs, 95)) * 1e6, 1),
                  "p99_us": None if not xs else round(float(np.percentile(xs, 99)) * 1e6, 1)}
    # throughput = completed queries per second over the phase (offered load is fixed at 10 q/s)
    thr = {}
    for ph in ("3-rebuild", "5-steady"):
        ts = [t for p, t, _ in h.query_lat if p == ph]
        thr[ph] = round(len(ts) / (max(ts) - min(ts)), 2) if len(ts) > 1 else None
    resources: dict = {}
    for ph in sorted({s["phase"] for s in samples}):
        ss = [s for s in samples if s["phase"] == ph]
        if len(ss) < 2 or "cg_cpu_usec" not in ss[0]:
            continue
        dt = ss[-1]["t"] - ss[0]["t"]
        d = lambda k: ss[-1].get(k, 0) - ss[0].get(k, 0)   # noqa: E731
        resources[ph] = {
            "seconds": round(dt, 1),
            "qdrant_cpu_cores_avg": round(d("cg_cpu_usec") / 1e6 / dt, 3),
            "qdrant_cpu_throttled_s": round(d("cg_cpu_throttled_usec") / 1e6, 2),
            "qdrant_io_read_MB": round(d("cg_io_rbytes") / 2**20, 1), "qdrant_io_write_MB": round(d("cg_io_wbytes") / 2**20, 1),
            "qdrant_io_read_ops_per_s": round(d("cg_io_rios") / dt, 1), "qdrant_io_write_ops_per_s": round(d("cg_io_wios") / dt, 1),
            "client_cpu_cores_avg": round(d("proc_cpu_ticks") / os.sysconf("SC_CLK_TCK") / dt, 3),
            "client_rss_peak_MB": round(max(x.get("proc_rss_kb", 0) for x in ss) / 1024, 1),
            "client_anon_peak_MB": round(max(x.get("proc_anon_kb", 0) for x in ss) / 1024, 1),
            "map_bytes_peak_MB": round(max(x.get("map_bytes", 0) for x in ss) / 2**20, 2),
            "host_mem_available_min_GB": round(min(x.get("host_mem_available_kb", 0) for x in ss) / 2**20, 2)}
    boundaries_equal = all(p.get("boundary", {}).get("equal", False) for p in report["phases"].values())
    p95_ok = all((lat[ph]["p95_ms"] is not None and lat[ph]["p95_ms"] <= 10.0) for ph in lat)
    report.update({
        "memory": {"by_phase": by_phase, "anon_peak_bytes": anon_peak, "anon_peak_gib": round(anon_peak / GiB, 4),
                   "memory_peak_bytes": max((s["peak"] for s in samples), default=0), "oom_kill": oom,
                   "limit_bytes": int((a.cgroup / "memory.max").read_text())
                   if (a.cgroup / "memory.max").exists() else None},
        "query_latency": lat, "map_lookup_latency": lk, "throughput_qps": thr, "resources": resources,
        "map_events": h.map_events, "map_miss_or_other_errors": len(h.errors),
        "live_write_latency_ms": {"n": len(wl), "p50": pct(wl, 50), "p95": pct(wl, 95), "p99": pct(wl, 99)},
        "errors": h.errors[:50], "error_count": len(h.errors),
        "pass_rules": {"no_oom_kill": oom == 0, "anon_peak_le_1_35_gib": anon_peak <= ANON_LIMIT,
                       "query_p95_le_10ms_phases_2_3_5": p95_ok, "exact_counts_equal_at_boundaries": boundaries_equal,
                       "phase3_reached_R_req": bool(report["phases"].get("3", {}).get("reached_R_req")),
                       "completed": "failure" not in report},
        "host": platform.node(), "samples": len(samples),
    })
    report["pass"] = all(report["pass_rules"].values())
    # The gate is one 1.5 GiB instance: a run under any other memory limit is decision evidence only and
    # must never satisfy CAP-8 (index.max_points_evidence requires gate_eligible).
    report["memory_limit_is_gate_limit"] = report["memory"]["limit_bytes"] == int(1.5 * GiB)
    report["gate_eligible"] = report["gate_eligible"] and report["memory_limit_is_gate_limit"]
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
