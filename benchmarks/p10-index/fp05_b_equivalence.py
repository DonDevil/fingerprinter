"""FP-05 Investigation B — correctness, interrupted-build recovery and REST/gRPC check for the payload-disk layout.

Compares collection layout A (original §13.3 configuration) with layout B (``--variant payload-disk``: payload
indexes on_disk=true) on the SAME points inside one dedicated container (never the dev instance), at 1x scale.

Checks (all recorded in the JSON, none silently dropped):
  1. exact counts equal and equal to the number of points sent (A and B);
  2. search equivalence on the 760 E-R1 queries (limit 100, score_threshold, quantization rescore, same payload keys):
     recall@100 against a brute-force exact top-100 (cosine floor applied) for A and for B, result-set overlap A vs B,
     score differences on common ids, payload equality of returned points;
  3. filter semantics on 60 subjects (``trep`` keyword + ``seq`` integer range): scroll/count results A vs B identical;
     delete-by-filter then exact count A vs B identical;
  4. recovery after an interrupted build: SIGKILL the container while layout B is being filled, restart, verify the
     collection loads and that re-sending the points (idempotent ids) gives exactly the expected count and the same
     recall;
  5. REST (JSON over HTTP) versus gRPC latency for A and B at the fixed E-R1 load (2 threads x 5 q/s), reported
     separately.
Run: capacity-harness venv, ``python -I benchmarks/p10-index/fp05_b_equivalence.py --vectors ... --container NAME --out FILE``.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

sys.path.insert(0, str(Path(__file__).resolve().parent))
import capacity_e_r1 as h1  # noqa: E402  (the unchanged E-R1 harness: same collection layout, payload and query code)

TOPK = 100
THRESH = 0.6


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300).stdout


def wait_ready(client, tries: int = 120) -> None:
    for _ in range(tries):
        try:
            client.get_collections()
            return
        except Exception:  # noqa: BLE001
            time.sleep(2)
    raise TimeoutError("qdrant did not come back")


def pct(xs, p):
    return round(float(np.percentile(xs, p)) * 1000, 3) if xs else None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--http-port", type=int, default=26333)
    ap.add_argument("--grpc-port", type=int, default=26334)
    ap.add_argument("--container", required=True)
    ap.add_argument("--rest-seconds", type=float, default=120.0)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--subset", type=int, help="first N vectors only (smoke test, never evidence)")
    a = ap.parse_args()

    base, Q = h1.load_corpus(a.vectors)
    base = base[: a.subset] if a.subset else base
    X = (base / np.linalg.norm(base, axis=1, keepdims=True)).astype(np.float32)
    n = len(X)
    ns = lambda variant: argparse.Namespace(variant=variant, indexing_threads=1, api_key=None,  # noqa: E731
                                            http_port=a.http_port, grpc_port=a.grpc_port)
    hA, hB = h1.Harness(ns("none")), h1.Harness(ns("payload-disk"))
    fields = "rep,cid,trep,tid,tver,seq,k,spec,lowinfo".split(",")
    m = hA.m
    from qdrant_client import QdrantClient

    rest = QdrantClient(host="127.0.0.1", port=a.http_port, prefer_grpc=False, https=False, timeout=600)
    rep: dict = {"benchmark": "FP05-B-equivalence", "points": n, "queries": len(Q), "score_threshold": THRESH, "checks": {}}
    params = m.SearchParams(quantization=m.QuantizationSearchParams(rescore=True, oversampling=2.0))

    def q(client, name, vec, **kw):
        return client.query_points(name, query=vec.tolist(), limit=TOPK, search_params=params, with_payload=fields,
                                   score_threshold=THRESH, **kw).points

    # 1. build both layouts from the same points
    for hx, name in ((hA, "eqA"), (hB, "eqB")):
        hx.create(name, "target")
        hx.bulk(name, "target", X)
    hA.wait_green("eqA"); hB.wait_green("eqB")
    cA = hA.client.count("eqA", exact=True).count
    cB = hB.client.count("eqB", exact=True).count
    rep["checks"]["counts"] = {"sent": n, "A": cA, "B": cB, "ok": cA == cB == n}
    rep["layout_B_payload_schema"] = {k: str(v.params) for k, v in hB.client.get_collection("eqB").payload_schema.items()}
    rep["layout_A_payload_schema"] = {k: str(v.params) for k, v in hA.client.get_collection("eqA").payload_schema.items()}

    # 2. search equivalence vs exact brute force
    Qn = Q / np.linalg.norm(Q, axis=1, keepdims=True)
    S = Qn @ X.T
    ids = [h1.pid(f"s{i // 12}", 1, i % 12) for i in range(n)]
    rec = {"A": [], "B": []}; overlap = []; sdiff = 0.0; payload_ok = True; empty = {"A": 0, "B": 0}
    for qi in range(len(Q)):
        order = np.argsort(-S[qi])[:TOPK]
        truth = {ids[j] for j in order if S[qi, j] >= THRESH}
        ra = q(hA.client, "eqA", Qn[qi]); rb = q(hB.client, "eqB", Qn[qi])
        sa = {str(p.id): p for p in ra}; sb = {str(p.id): p for p in rb}
        for k, s in (("A", sa), ("B", sb)):
            if not s: empty[k] += 1
            if truth: rec[k].append(len(truth & set(s)) / len(truth))
        union = set(sa) | set(sb)
        overlap.append(len(set(sa) & set(sb)) / len(union) if union else 1.0)
        for i in set(sa) & set(sb):
            sdiff = max(sdiff, abs(sa[i].score - sb[i].score))
            payload_ok &= sa[i].payload == sb[i].payload
    rep["checks"]["search"] = {
        "recall_at_100_A_mean": round(float(np.mean(rec["A"])), 5), "recall_at_100_B_mean": round(float(np.mean(rec["B"])), 5),
        "recall_min_A": round(float(np.min(rec["A"])), 4), "recall_min_B": round(float(np.min(rec["B"])), 4),
        "queries_with_ground_truth": len(rec["A"]), "empty_result_queries": empty,
        "jaccard_A_vs_B_mean": round(float(np.mean(overlap)), 5), "jaccard_A_vs_B_min": round(float(np.min(overlap)), 4),
        "max_abs_score_diff_common_ids": sdiff, "payload_equal_on_common_ids": bool(payload_ok),
        "ok": bool(np.mean(rec["B"]) >= np.mean(rec["A"]) - 0.005 and payload_ok)}

    # 3. filter semantics + delete by filter
    rng = np.random.default_rng(7)
    subjects = [f"s{int(i)}" for i in rng.choice(n // 12, 60, replace=False)]
    mism = 0
    for s in subjects:
        flt = m.Filter(must=[m.FieldCondition(key="trep", match=m.MatchValue(value=s)),
                             m.FieldCondition(key="seq", range=m.Range(lte=1))])
        ia = sorted(str(p.id) for p in hA.client.scroll("eqA", scroll_filter=flt, limit=100, with_payload=False)[0])
        ib = sorted(str(p.id) for p in hB.client.scroll("eqB", scroll_filter=flt, limit=100, with_payload=False)[0])
        ca = hA.client.count("eqA", count_filter=flt, exact=True).count; cb = hB.client.count("eqB", count_filter=flt, exact=True).count
        mism += int(ia != ib or ca != cb or ca != 12)
    gone = 0
    for s in subjects[:20]:
        flt = m.Filter(must=[m.FieldCondition(key="trep", match=m.MatchValue(value=s)),
                             m.FieldCondition(key="seq", range=m.Range(lt=2))])
        for hx, name in ((hA, "eqA"), (hB, "eqB")):
            hx.client.delete(name, points_selector=m.FilterSelector(filter=flt), wait=True)
        gone += 12
    cA2 = hA.client.count("eqA", exact=True).count; cB2 = hB.client.count("eqB", exact=True).count
    rep["checks"]["filters"] = {"subjects": len(subjects), "mismatches": mism, "deleted_per_layout": gone,
                                "count_after_delete_A": cA2, "count_after_delete_B": cB2,
                                "ok": mism == 0 and cA2 == cB2 == n - gone}

    # 4. recovery after an interrupted build (layout B)
    hB.create("eqC", "target")
    hB.resident["eqC"] = 0
    stop = threading.Event(); sent = {"n": 0}

    def fill():
        try:
            batch = []
            for i in range(n):
                batch.append((f"s{i // 12}", 1, i % 12, X[i]))
                if len(batch) == 1000:
                    hB.upsert("eqC", "target", batch); sent["n"] += 1000; batch = []
                if stop.is_set(): return
        except Exception:  # noqa: BLE001 - expected when the container is killed
            return
    t = threading.Thread(target=fill, daemon=True); t.start()
    while sent["n"] < 40_000 and t.is_alive(): time.sleep(0.2)
    sh("docker", "kill", "--signal", "KILL", a.container); stop.set(); t.join(timeout=60)
    acked = sent["n"]
    sh("docker", "start", a.container)
    hB2 = h1.Harness(ns("payload-disk")); wait_ready(hB2.client)
    names = {c.name for c in hB2.client.get_collections().collections}
    after_crash = hB2.client.count("eqC", exact=True).count if "eqC" in names else None
    loaded_others = all(k in names for k in ("eqA", "eqB"))
    cB3 = hB2.client.count("eqB", exact=True).count
    hB2.resident["eqC"] = 0
    batch = []
    for i in range(n):  # re-send everything: ids are deterministic, so this is an idempotent upsert
        batch.append((f"s{i // 12}", 1, i % 12, X[i]))
        if len(batch) == 1000: hB2.upsert("eqC", "target", batch); batch = []
    if batch: hB2.upsert("eqC", "target", batch)
    hB2.wait_green("eqC")
    cC = hB2.client.count("eqC", exact=True).count
    # recall after recovery
    rc = []
    for qi in range(0, len(Q), 4):
        order = np.argsort(-S[qi])[:TOPK]
        truth = {ids[j] for j in order if S[qi, j] >= THRESH}
        if truth: rc.append(len(truth & {str(p.id) for p in q(hB2.client, "eqC", Qn[qi])}) / len(truth))
    rep["checks"]["recovery"] = {"acknowledged_before_kill": acked, "count_after_restart": after_crash,
                                 "other_collections_loaded": loaded_others, "eqB_count_after_restart": cB3,
                                 "count_after_resend": cC, "expected": n, "recall_at_100_after_recovery": round(float(np.mean(rc)), 5),
                                 "ok": bool(loaded_others and cB3 == n - gone and cC == n and (after_crash is None or after_crash >= 0)
                                            and np.mean(rc) >= np.mean(rec["A"]) - 0.01)}

    # 5. REST vs gRPC at the fixed load (2 threads x 5 q/s); A = eqA (all points minus 240 deleted), B = eqB
    def load(client, name, seconds):
        lat: list[float] = []; errs: list[str] = []
        stopx = time.time() + seconds

        def worker(seed):
            rng2 = np.random.default_rng(seed); nxt = time.time()
            while time.time() < stopx:
                v = Qn[int(rng2.integers(len(Qn)))]; t0 = time.time()
                try:
                    q(client, name, v); lat.append(time.time() - t0)
                except Exception as exc:  # noqa: BLE001
                    errs.append(str(exc)[:200])
                nxt += 0.2; time.sleep(max(0.0, nxt - time.time()))
        ts = [threading.Thread(target=worker, args=(s,)) for s in (1, 2)]
        [x.start() for x in ts]; [x.join() for x in ts]
        lat = lat[int(len(lat) * 0.15):]  # drop first 15% as warm-up
        return {"n": len(lat), "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99), "errors": len(errs)}
    for tag, cl in (("grpc", hB2.client), ("rest", rest)):
        rep.setdefault("latency", {})[tag] = {}
        for name in ("eqA", "eqB"):
            rep["latency"][tag][name] = load(cl, name, a.rest_seconds)
    rep["checks"]["all_ok"] = all(v.get("ok") for v in rep["checks"].values() if isinstance(v, dict) and "ok" in v)
    a.out.write_text(json.dumps(rep, indent=1, default=str))
    print(json.dumps({k: v for k, v in rep.items() if k != "layout_A_payload_schema"}, indent=1, default=str))


if __name__ == "__main__":
    main()
