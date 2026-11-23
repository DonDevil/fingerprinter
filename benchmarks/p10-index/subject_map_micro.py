"""FP-05 Investigation A: lookup-cost / RAM micro-benchmark of the candidate sidecar designs (eliminates options).

Options (design-analysis.md): A naive in-process dict(id -> payload dict) and A2 dict(id -> tuple);
B the compact columnar ``SubjectMap`` (in RAM, and memory-mapped from its checkpoint); C a separate local
process answering over a Unix socket (a lower bound: the server is a bare dict, pickle framing).
Each lookup resolves 100 ids (one query's hits), 2,000 repetitions, ids drawn at random; every option must
return the same metadata as the reference payloads. Output: JSON on stdout. Run with the E-R1 venv:

    python -I benchmarks/p10-index/subject_map_micro.py --points 626658 --out results.json
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import pickle
import socket
import struct
import sys
import tempfile
import time
import uuid
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fingerprinter.subjectmap import SubjectMap  # noqa: E402

NS = uuid.uuid5(uuid.NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")
SPEC = "sha256:e-r1"


def rss_mb() -> float:
    for line in open("/proc/self/status"):
        if line.startswith("VmRSS"):
            return int(line.split()[1]) / 1024
    return 0.0


def pctl(xs, p):
    return round(float(np.percentile(xs, p)) * 1e6, 1)  # microseconds


def payload(subject: str, seq: int, k: int) -> dict:
    return {"rep": "rep:" + subject, "cid": "cnt:" + subject, "spec": SPEC, "k": k, "seq": seq, "trep": subject,
            "tid": "tgt:" + subject, "tver": 1, "lowinfo": False}


def serve(path: str, table: dict, ready) -> None:
    srv = socket.socket(socket.AF_UNIX)
    srv.bind(path)
    srv.listen(1)
    ready.set()
    c, _ = srv.accept()
    while True:
        hdr = c.recv(4, socket.MSG_WAITALL)
        if not hdr:
            return
        body = c.recv(struct.unpack("!I", hdr)[0], socket.MSG_WAITALL)
        ids = pickle.loads(body)
        out = pickle.dumps([table.get(i) for i in ids])
        c.sendall(struct.pack("!I", len(out)) + out)


def timed(fn, batches, reps):
    lat = []
    for r in range(reps):
        b = batches[r % len(batches)]
        t = time.perf_counter()
        fn(b)
        lat.append(time.perf_counter() - t)
    return {"p50_us": pctl(lat, 50), "p95_us": pctl(lat, 95), "p99_us": pctl(lat, 99)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--points", type=int, default=626_658)
    ap.add_argument("--reps", type=int, default=2000)
    ap.add_argument("--out", type=Path)
    ap.add_argument("--option", choices=["A", "A2", "B", "C"], required=True,
                    help="one option per process so that RSS deltas are not polluted by earlier allocations")
    a = ap.parse_args()
    n = a.points
    ids, pls = [], []
    for i in range(n):
        s, k = f"s{i // 12}", i % 12
        ids.append(str(uuid.uuid5(NS, f"{s}:1:{k}")))
        pls.append((s, 1, k))
    rng = np.random.default_rng(1)
    batches = [[ids[j] for j in rng.integers(0, n, 100)] for _ in range(200)]
    res: dict = {"points": n, "lookup_ids": 100, "reps": a.reps, "options": {}}

    opt = a.option
    idx_of = {}
    for b in batches[:2]:
        for i in b:
            idx_of[i] = None
    for j, i in enumerate(ids):
        if i in idx_of:
            idx_of[i] = j
    ref = {i: payload(*pls[j]) for i, j in idx_of.items()}
    base_rss = rss_mb()
    if opt == "A":
        d = {i: payload(*p) for i, p in zip(ids, pls, strict=True)}
        res["options"]["A_dict_of_payload_dicts"] = {**timed(lambda b: [d.get(i) for i in b], batches, a.reps),
                                                     "rss_delta_mb": round(rss_mb() - base_rss, 1)}
    elif opt == "A2":
        subj: dict = {}
        d2 = {}
        for i, (s, q, k) in zip(ids, pls, strict=True):
            d2[i] = (subj.setdefault(s, ("rep:" + s, "cnt:" + s, s, "tgt:" + s)), q, k)

        def build(b):
            out = []
            for i in b:
                (rep, cid, trep, tid), q, k = d2[i]
                out.append({"rep": rep, "cid": cid, "spec": SPEC, "k": k, "seq": q, "trep": trep, "tid": tid,
                            "tver": 1, "lowinfo": False})
            return out

        assert build(batches[0]) == [ref[i] for i in batches[0]]
        res["options"]["A2_dict_of_tuples"] = {**timed(build, batches, a.reps),
                                               "rss_delta_mb": round(rss_mb() - base_rss, 1)}
    elif opt == "B":
        sm = SubjectMap("c_g1", spec=SPEC, role="target", generation=1)
        t = time.perf_counter()
        chunk = []
        for i, (s, q, k) in enumerate(pls):
            chunk.append((ids[i], payload(s, q, k)))
            if len(chunk) == 5000:
                sm.record_many(chunk, "trep")
                chunk = []
        sm.record_many(chunk, "trep")
        build_s = time.perf_counter() - t
        del chunk
        with tempfile.TemporaryDirectory() as tmp:
            t = time.perf_counter()
            sm.save(Path(tmp))
            save_s = time.perf_counter() - t
            disk = sum(f.stat().st_size for f in (Path(tmp) / "c_g1").iterdir())
            sm.close()
            del sm
            for mm in (False, True):
                import gc
                gc.collect()
                base_rss = rss_mb()
                t = time.perf_counter()
                m = SubjectMap.load(Path(tmp), "c_g1", spec=SPEC, role="target", generation=1, mmap=mm)
                load_s = time.perf_counter() - t
                assert m.get(batches[0]) == [ref[i] for i in batches[0]], "SubjectMap differs from the reference"
                m.get(batches[1])
                r = {**timed(m.get, batches, a.reps), "rss_delta_mb": round(rss_mb() - base_rss, 1),
                     "nbytes_columns_mb": round(sum(x.nbytes for x in m._base.values()) / 2**20, 2),
                     "disk_mb": round(disk / 2**20, 2), "load_s": round(load_s, 3),
                     "build_record_s": round(build_s, 2), "save_s": round(save_s, 2)}
                res["options"]["B_columnar_mmap" if mm else "B_columnar_ram"] = r
                m.close()
                del m
    else:
        table = {i: (pls[j][0], pls[j][1], pls[j][2]) for j, i in enumerate(ids)}
        path = os.path.join(tempfile.mkdtemp(), "sock")
        ev = mp.Event()
        proc = mp.Process(target=serve, args=(path, table, ev), daemon=True)
        proc.start()
        ev.wait(30)
        c = socket.socket(socket.AF_UNIX)
        c.connect(path)

        def rpc(b):
            body = pickle.dumps(b)
            c.sendall(struct.pack("!I", len(body)) + body)
            hdr = c.recv(4, socket.MSG_WAITALL)
            return pickle.loads(c.recv(struct.unpack("!I", hdr)[0], socket.MSG_WAITALL))

        res["options"]["C_unix_socket_process"] = {**timed(rpc, batches, a.reps),
                                                   "note": "bare dict server, pickle framing, no payload build, "
                                                           "server RSS not included"}
        c.close()
        proc.terminate()
    res["host"] = os.uname().nodename
    out = json.dumps(res, indent=1)
    print(out)
    if a.out:
        a.out.write_text(out)


if __name__ == "__main__":
    main()
