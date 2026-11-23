"""FP-05 Investigation A: bounded REST-path validation (1x corpus only; NOT a gate run).

Sends the exact JSON body QdrantIndex.search_batch builds (POST /collections/<c>/points/search/batch, limit 100, rescore,
oversampling 2, score_threshold 0.6, accept-encoding identity, keep-alive) with http.client, alternating
baseline (9 payload fields) and map (with_payload false + SubjectMap lookup) windows at 2 threads x 5 q/s.
Run: python -I subject_map_rest.py --vectors DIR --http-port P --grpc-port G --out FILE   (own Qdrant container, empty data)
"""
from __future__ import annotations
import argparse, http.client, json, sys, threading, time, uuid
from pathlib import Path
import numpy as np
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from fingerprinter.subjectmap import SubjectMap  # noqa: E402

NS = uuid.uuid5(uuid.NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")
FIELDS = ["rep", "cid", "trep", "tid", "tver", "seq", "k", "spec", "lowinfo"]


def load(v: Path):
    base, qs = [], []
    for sub in ("soak-1000", "l-pairs"):
        for f in sorted((v / sub).glob("*.npz")):
            z = np.load(f)
            (qs if f.name.startswith("targets__") else base).append(z["vectors"].astype(np.float32))
    return np.vstack(base), np.vstack(qs)


class Conn:
    def __init__(self, port):
        self.c = http.client.HTTPConnection("127.0.0.1", port, timeout=30)

    def call(self, method, path, body=None):
        self.c.request(method, path, None if body is None else json.dumps(body),
                       {"content-type": "application/json", "accept-encoding": "identity"})
        r = self.c.getresponse()
        raw = r.read()
        if r.status >= 300:
            raise RuntimeError(f"{path}: {r.status} {raw[:200]!r}")
        return json.loads(raw), len(raw)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--http-port", type=int, default=36333)
    ap.add_argument("--window-s", type=int, default=120)
    ap.add_argument("--rounds", type=int, default=3)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    X, Q = load(a.vectors)
    X /= np.linalg.norm(X, axis=1, keepdims=True)
    c = Conn(a.http_port)
    name = "g1"
    c.call("PUT", f"/collections/{name}", {
        "vectors": {"size": 768, "distance": "Dot", "on_disk": True},
        "hnsw_config": {"m": 32, "ef_construct": 200, "max_indexing_threads": 1},
        "quantization_config": {"scalar": {"type": "int8", "always_ram": True}},
        "on_disk_payload": True, "optimizers_config": {"max_optimization_threads": 1}})
    for f in ("rep", "trep", "cid", "tid", "spec"):
        c.call("PUT", f"/collections/{name}/index?wait=true", {"field_name": f, "field_schema": "keyword"})
    c.call("PUT", f"/collections/{name}/index?wait=true", {"field_name": "seq", "field_schema": "integer"})
    sm = SubjectMap(name, spec="sha256:e-r1", role="target", generation=1)
    t0 = time.time()
    for s in range(0, len(X), 500):
        pts = []
        for i in range(s, min(s + 500, len(X))):
            sub, k = f"s{i // 12}", i % 12
            pts.append({"id": str(uuid.uuid5(NS, f"{sub}:1:{k}")), "vector": X[i].tolist(),
                        "payload": {"rep": "rep:" + sub, "cid": "cnt:" + sub, "spec": "sha256:e-r1", "k": k, "t": 5.0 * k,
                                    "seq": 1, "trep": sub, "tid": "tgt:" + sub, "tver": 1, "lowinfo": False, "lowinfo_rev": 1}})
        sm.record_many(((p["id"], p["payload"]) for p in pts), "trep")
        c.call("PUT", f"/collections/{name}/points?wait=true", {"points": pts})
    ingest_s = time.time() - t0
    while c.call("GET", f"/collections/{name}")[0]["result"]["status"] != "green":
        time.sleep(2)
    n = c.call("POST", f"/collections/{name}/points/count", {"exact": True})[0]["result"]["count"]
    assert n == len(sm) == len(X), (n, len(sm), len(X))
    params = {"exact": False, "hnsw_ef": None, "quantization": {"ignore": False, "rescore": True, "oversampling": 2.0}}
    params.pop("hnsw_ef")

    def body(q, mode):
        return {"searches": [{"vector": q.tolist(), "limit": 100, "params": params,
                              "with_payload": FIELDS if mode == "base" else False, "score_threshold": 0.6}]}

    # identity: same ids/scores/metadata over all queries
    bad = 0
    keep = set(FIELDS)
    for q in Q[:200]:
        ra, _ = c.call("POST", f"/collections/{name}/points/search/batch", body(q, "base"))
        rb, _ = c.call("POST", f"/collections/{name}/points/search/batch", body(q, "map"))
        A, B = ra["result"][0], rb["result"][0]
        metas = sm.get([h["id"] for h in B])
        if [(h["id"], h["score"]) for h in A] != [(h["id"], h["score"]) for h in B] or \
           [{k: v for k, v in h["payload"].items() if k in keep} for h in A] != metas:
            bad += 1
    out = {"corpus": len(X), "ingest_s": round(ingest_s, 1), "identity_queries": 200, "identity_mismatches": bad,
           "windows": []}

    def window(mode, seconds):
        lat, look, sizes, errs = [], [], [], []
        stop = time.time() + seconds

        def worker(seed):
            cc = Conn(a.http_port)
            rng = np.random.default_rng(seed)
            nxt = time.time()
            while time.time() < stop:
                q = Q[int(rng.integers(len(Q)))]
                t = time.time()
                try:
                    r, nb = cc.call("POST", f"/collections/{name}/points/search/batch", body(q, mode))
                    hits = r["result"][0]
                    if mode == "map":
                        tl = time.perf_counter()
                        metas = sm.get([h["id"] for h in hits])
                        look.append(time.perf_counter() - tl)
                        if any(m is None for m in metas):
                            errs.append("miss")
                    lat.append(time.time() - t)
                    sizes.append(nb)
                except Exception as e:  # noqa: BLE001
                    errs.append(repr(e)[:200])
                nxt += 0.2
                time.sleep(max(0.0, nxt - time.time()))

        th = [threading.Thread(target=worker, args=(s,)) for s in (1, 2)]
        [x.start() for x in th]
        [x.join() for x in th]
        p = lambda xs, q: round(float(np.percentile(xs, q)) * 1000, 3) if xs else None  # noqa: E731
        return {"mode": mode, "n": len(lat), "p50_ms": p(lat, 50), "p95_ms": p(lat, 95), "p99_ms": p(lat, 99),
                "lookup_p95_ms": p(look, 95), "resp_bytes_mean": int(np.mean(sizes)) if sizes else None, "errors": errs[:5],
                "error_count": len(errs)}

    window("base", 20)  # warm
    for r in range(a.rounds):
        for mode in (("base", "map") if r % 2 == 0 else ("map", "base")):
            w = window(mode, a.window_s)
            w["round"] = r
            out["windows"].append(w)
            print(json.dumps(w), flush=True)
    a.out.write_text(json.dumps(out, indent=1))
    print(json.dumps({k: v for k, v in out.items() if k != "windows"}))


if __name__ == "__main__":
    main()
