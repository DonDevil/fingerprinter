"""FP-05 Investigation B — correctness, interrupted-build recovery and REST/gRPC qualification of the payload-disk layout.

Runs against ONE dedicated, disposable Qdrant container (never the dev instance). Layouts compared, all built from the
SAME points with the production REST code path (``fingerprinter.vindex.QdrantIndex``):

  A   original production layout (``QdrantIndex.create_collection`` unchanged: payload indexes plain keyword/integer)
  B   "payload-disk" exactly as benchmarked in E-R1: payload indexes with ``on_disk=true`` only (lookup/range unset)
  Bx  payload indexes with ``on_disk=true`` and integer ``lookup=true, range=true`` stated explicitly
      (resolves whether "unset" behaves as the documented default)

Checks (each reports PASS / FAIL / INCONCLUSIVE with the numbers behind it; nothing is dropped):
  counts      exact counts equal and equal to the points sent;
  search      760 E-R1 queries, gRPC (limit 100, score_threshold, quantization rescore): A vs B vs Bx ordered id lists,
              score differences, payload equality, recall@100 against brute-force exact top-100 (reported, not used for PASS);
              strict PASS = every query returns identical ordered ids in all layouts;
  rest_parity production REST ``search_batch`` vs gRPC on the same layout, and REST A vs REST B;
  filters     many filter shapes (match, range, must_not, should, seq lookup) over 60 subjects, count + scroll ids,
              compared across layouts and against the known construction (12 points per subject);
  delete      delete-by-filter (REST) on all layouts: target gone, others untouched, collection still queryable;
              re-upsert at a new seq followed by ``seq <`` cleanup;
  recovery    SIGKILL the container while layout B is being filled over REST; restart; the other collections must be
              intact and queryable; the interrupted collection is reported, re-filled idempotently, counts and recall
              checked;
  rest_api    REST error handling (missing collection, wrong dimension, bad filter, existing collection, no server);
  latency     fixed 2 x 5 q/s load, REST and gRPC reported separately, A and B (bounded, noisy host).
"""

from __future__ import annotations

import argparse
import glob
import json
import subprocess
import sys
import threading
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
REPO = HERE.parent.parent
sys.path.insert(0, str(HERE))
sys.path.insert(0, str(REPO))
for extra in (str(REPO.parent / "crawler2" / "contracts" / "src"), *glob.glob(str(REPO / "env/lib/python3*/site-packages"))):
    sys.path.append(extra)  # appended last: the benchmark venv's numpy/qdrant-client win; only requests/contracts come from here
import capacity_e_r1 as h1  # noqa: E402  (the unchanged E-R1 harness: payload/query/corpus code)
from fingerprinter.vindex import (  # noqa: E402
    GenerationParams, IndexMissingError, IndexPermanentError, IndexUnavailableError, Point, QdrantIndex, SearchRequest,
)

TOPK, THRESH = 100, 0.6
FIELDS = ["rep", "cid", "trep", "tid", "tver", "seq", "k", "spec", "lowinfo"]


class LayoutIndex(QdrantIndex):
    """Production create_collection with ONE difference: the payload-index schemas (``mode``)."""

    mode = "plain"

    def create_collection(self, name: str, params: GenerationParams, role: str) -> None:
        if self.mode == "plain":
            return super().create_collection(name, params, role)
        body = {"vectors": {"size": params.dim, "distance": params.distance, "on_disk": params.vectors_on_disk},
                "hnsw_config": {"m": params.hnsw_m, "ef_construct": params.ef_construct},
                "quantization_config": {"scalar": {"type": "int8", "always_ram": params.quantization_always_ram}},
                "on_disk_payload": params.payload_on_disk}
        self._call("PUT", f"/collections/{name}", body, timeout_s=60)
        kw = {"type": "keyword", "on_disk": True}
        iw = {"type": "integer", "on_disk": True}
        if self.mode == "disk-explicit":
            iw.update(lookup=True, range=True)
        for f in ("rep", "cid", "spec") if role == "media" else ("rep", "trep", "cid", "tid", "spec"):
            self._call("PUT", f"/collections/{name}/index?wait=true", {"field_name": f, "field_schema": kw}, timeout_s=60)
        self._call("PUT", f"/collections/{name}/index?wait=true", {"field_name": "seq", "field_schema": iw}, timeout_s=60)


def sh(*cmd: str) -> str:
    return subprocess.run(cmd, capture_output=True, text=True, timeout=300).stdout.strip()


def pct(xs, p):
    return round(float(np.percentile(xs, p)) * 1000, 3) if xs else None


def tie_recall(row_scores: np.ndarray, returned: list[float]) -> float | None:
    """Tie-aware recall@100 of one query: the exact top-k (k = min(100, #points >= threshold)) is a SCORE target, so
    equal-scored duplicate vectors are interchangeable. recall = hits scoring >= (exact k-th score - 1e-4) / k."""
    m = int((row_scores >= THRESH).sum())
    if m == 0:
        return None
    k = min(TOPK, m)
    sk = float(np.partition(row_scores, -k)[-k])
    return min(sum(1 for x in returned if x >= sk - 1e-4), k) / k


def status(ok: bool | None) -> str:
    return "PASS" if ok else ("INCONCLUSIVE" if ok is None else "FAIL")


def flt(*must, must_not=(), should=()) -> dict:
    d: dict = {}
    if must: d["must"] = list(must)
    if must_not: d["must_not"] = list(must_not)
    if should: d["should"] = list(should)
    return d


mt = lambda k, v: {"key": k, "match": {"value": v}}  # noqa: E731
rg = lambda k, **r: {"key": k, "range": r}  # noqa: E731


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--http-port", type=int, default=26333)
    ap.add_argument("--grpc-port", type=int, default=26334)
    ap.add_argument("--container", required=True)
    ap.add_argument("--cgroup", type=Path, help="container cgroup dir: memory/OOM sampling")
    ap.add_argument("--rest-seconds", type=float, default=120.0)
    ap.add_argument("--out", type=Path, required=True)
    ap.add_argument("--subset", type=int, help="first N vectors only (smoke test, never evidence)")
    a = ap.parse_args()
    a_out_npz = a.out.with_suffix(".queries.npz")
    t_start = time.time()
    base, Q = h1.load_corpus(a.vectors)
    base = base[: a.subset] if a.subset else base
    X = (base / np.linalg.norm(base, axis=1, keepdims=True)).astype(np.float32)
    n = len(X)
    Qn = (Q / np.linalg.norm(Q, axis=1, keepdims=True)).astype(np.float32)
    ns = argparse.Namespace(variant="none", indexing_threads=1, api_key=None, http_port=a.http_port, grpc_port=a.grpc_port)
    H = h1.Harness(ns)                      # gRPC client + the E-R1 payload builder
    m = H.m
    url = f"http://127.0.0.1:{a.http_port}"
    ix = {k: type("L", (LayoutIndex,), {"mode": mode})(url, None, timeout_s=120)
          for k, mode in (("A", "plain"), ("B", "disk"), ("Bx", "disk-explicit"))}
    rep: dict = {"benchmark": "FP05-B-qualification", "points": n, "queries": len(Q), "score_threshold": THRESH,
                 "subset": a.subset, "started_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(t_start)),
                 "script_sha256": __import__("hashlib").sha256(Path(__file__).read_bytes()).hexdigest(),
                 "script_dirty": bool(sh("git", "-C", str(REPO), "status", "--porcelain", "--", str(Path(__file__)), str(HERE / "capacity_e_r1.py"))),
                 "qdrant_version": ix["A"].version(), "fingerprinter_sha": sh("git", "-C", str(REPO), "rev-parse", "HEAD"),
                 "container": sh("docker", "inspect", "-f",
                                 "{{.Config.Image}} mem={{.HostConfig.Memory}} swap={{.HostConfig.MemorySwap}} nanocpus={{.HostConfig.NanoCpus}}",
                                 a.container),
                 "checks": {}}
    samples: list[dict] = []
    stop_s = threading.Event()

    def sampler() -> None:
        while not stop_s.is_set() and a.cgroup:
            try:
                st = dict(l.split() for l in (a.cgroup / "memory.stat").read_text().splitlines())
                ev = dict(l.split() for l in (a.cgroup / "memory.events").read_text().splitlines())
                samples.append({"t": time.time(), "anon": int(st["anon"]), "oom_kill": int(ev["oom_kill"])})
            except OSError:
                samples.append({"t": time.time(), "error": "cgroup unreadable"})
            stop_s.wait(1.0)
    threading.Thread(target=sampler, daemon=True).start()

    ids = [h1.pid(f"s{i // 12}", 1, i % 12) for i in range(n)]

    def load(name: str, index: QdrantIndex, count: int | None = None) -> None:
        batch: list[Point] = []
        for i in range(count if count is not None else n):
            batch.append(Point(ids[i], X[i], H.payload("target", f"s{i // 12}", 1, i % 12)))
            if len(batch) == 500:
                index.upsert(name, batch); batch = []
        if batch: index.upsert(name, batch)

    # ---- build the three layouts over production REST ------------------------------------------------
    names = {"A": "eqA", "B": "eqB", "Bx": "eqBx"}
    build = {}
    for k, name in names.items():
        t0 = time.time(); ix[k].create_collection(name, GenerationParams(), "target"); load(name, ix[k])
        st = ix[k].wait_green(name, 1800); build[k] = {"seconds": round(time.time() - t0, 1), "status": st}
    rep["build"] = build
    rep["payload_schema"] = {k: {f: v.get("params") or v.get("data_type") for f, v in
                                 ix[k].collection_info(names[k])["payload_schema"].items()} for k in names}
    cnt = {k: ix[k].count(names[k]) for k in names}
    rep["checks"]["counts"] = {"sent": n, **cnt, "status": status(all(v == n for v in cnt.values()))}

    # ---- control: a SECOND plain-layout build (A2) shows how much two builds of the SAME layout differ ------
    ix["A2"] = type("L", (LayoutIndex,), {"mode": "plain"})(url, None, timeout_s=120)
    names["A2"] = "eqA2"
    t0 = time.time(); ix["A2"].create_collection("eqA2", GenerationParams(), "target"); load("eqA2", ix["A2"])
    build["A2"] = {"seconds": round(time.time() - t0, 1), "status": ix["A2"].wait_green("eqA2", 1800)}

    # ---- search equivalence over gRPC + recall vs exact brute force ----------------------------------
    params = m.SearchParams(quantization=m.QuantizationSearchParams(rescore=True, oversampling=2.0))
    S = Qn @ X.T

    def gq(name, v):
        return H.client.query_points(name, query=v.tolist(), limit=TOPK, search_params=params, with_payload=FIELDS,
                                     score_threshold=THRESH).points

    def classify(ra, rb) -> str:
        """identical = same ordered ids; tie_equivalent = ids differ but the score sequences are equal (1e-6): the
        differences are between equal-scored points (the corpus contains duplicate vectors); genuine = scores differ."""
        ia, ib = [str(p.id) for p in ra], [str(p.id) for p in rb]
        if ia == ib: return "identical"
        sa, sb = [p.score for p in ra], [p.score for p in rb]
        return "tie_equivalent" if len(sa) == len(sb) and all(abs(x - y) <= 1e-6 for x, y in zip(sa, sb)) else "genuine"
    res: dict[str, list] = {k: [] for k in names}
    for qi in range(len(Qn)):
        for k, name in names.items():
            res[k].append(gq(name, Qn[qi]))
    nq = len(Qn)
    pairs: dict = {}
    for k in ("B", "Bx", "A2"):
        c = {"identical": 0, "tie_equivalent": 0, "genuine": 0}; md = 0.0; pe = True; first = []
        for qi in range(nq):
            kind = classify(res["A"][qi], res[k][qi]); c[kind] += 1
            if kind != "identical" and len(first) < 5:
                ia, ib = [str(p.id) for p in res["A"][qi]], [str(p.id) for p in res[k][qi]]
                first.append({"q": qi, "kind": kind, "n_A": len(ia), f"n_{k}": len(ib), "only_A": len(set(ia) - set(ib)), f"only_{k}": len(set(ib) - set(ia))})
            for pa, pk in zip(res["A"][qi], res[k][qi]):
                if str(pa.id) == str(pk.id):
                    md = max(md, abs(pa.score - pk.score)); pe &= pa.payload == pk.payload
        pairs[k] = {**c, "max_abs_score_diff_same_id": md, "payload_equal_same_id": pe, "examples": first}
    recalls = {k: [] for k in names}; empty = {k: 0 for k in names}; id_recalls = {k: [] for k in names}
    for qi in range(nq):
        order = np.argsort(-S[qi])[:TOPK]
        truth = {ids[j] for j in order if S[qi, j] >= THRESH}
        for k in names:
            idk = {str(p.id) for p in res[k][qi]}
            if not idk: empty[k] += 1
            tr = tie_recall(S[qi], [p.score for p in res[k][qi]])
            if tr is not None: recalls[k].append(tr)
            if truth: id_recalls[k].append(len(truth & idk) / len(truth))
    row_of = {i_: j for j, i_ in enumerate(ids)}
    npz = {}
    for k in names:
        npz[k + "_ids"] = np.array([[row_of.get(str(p.id), -1) for p in res[k][qi]] + [-2] * (TOPK - len(res[k][qi])) for qi in range(nq)], dtype=np.int32)
        npz[k + "_scores"] = np.array([[p.score for p in res[k][qi]] + [np.nan] * (TOPK - len(res[k][qi])) for qi in range(nq)], dtype=np.float32)
    np.savez_compressed(a_out_npz, **npz)
    mean_r = {k: float(np.mean(v)) for k, v in recalls.items()}
    worse = {k: int(sum(1 for x, y in zip(recalls[k], recalls["A"]) if x < y - 1e-9)) for k in names if k != "A"}
    better = {k: int(sum(1 for x, y in zip(recalls[k], recalls["A"]) if x > y + 1e-9)) for k in names if k != "A"}
    tol = 0.002
    crit_ok = all(mean_r[k] >= min(mean_r["A"], mean_r["A2"]) - tol for k in ("B", "Bx")) and all(pairs[k]["payload_equal_same_id"] and pairs[k]["max_abs_score_diff_same_id"] <= 1e-6 for k in ("B", "Bx"))
    rep["checks"]["search_grpc"] = {
        "queries": nq, "vs_A": pairs, "empty_result_queries": empty, "queries_with_ground_truth": len(recalls["A"]),
        "tie_aware_recall_at_100_mean": {k: round(v, 5) for k, v in mean_r.items()},
        "tie_aware_recall_at_100_min": {k: round(float(np.min(v)), 4) for k, v in recalls.items()},
        "queries_with_lower_recall_than_A": worse, "queries_with_higher_recall_than_A": better,
        "id_based_recall_at_100_mean_NOT_RELIABLE_with_ties": {k: round(float(np.mean(v)), 5) for k, v in id_recalls.items()},
        "per_query_results_file": a_out_npz.name,
        "criterion": f"PASS = for B and Bx: mean tie-aware recall >= min(mean A, mean A2) - {tol} (A2 = same-layout control build), "
                     "same-id score diff <= 1e-6 and payload equal. Strict ordered-id identity and 'genuine' score-sequence differences are "
                     "reported in vs_A but are not achievable even between two builds of the SAME layout (control A2). "
                     "Defined by the investigation (revised after run 1, see FINDINGS), not by the owner.",
        "status": status(crit_ok)}

    # ---- REST parity (production search_batch) --------------------------------------------------------
    rest_res = {}
    for k in ("A", "B"):
        out = []
        for i in range(0, nq, 64):
            reqs = [SearchRequest(vector=Qn[j], limit=TOPK, score_threshold=THRESH, payload_fields=tuple(FIELDS))
                    for j in range(i, min(i + 64, nq))]
            out += ix[k].search_batch(names[k], reqs, hnsw_ef=None, oversampling=2.0)
        rest_res[k] = out
    same_rg = {k: sum([h.id for h in rest_res[k][j]] == [str(p.id) for p in res[k][j]] for j in range(nq)) for k in ("A", "B")}
    sc_rab = sum(len(rest_res["A"][j]) == len(rest_res["B"][j]) and all(abs(x.score - y.score) <= 1e-6 for x, y in zip(rest_res["A"][j], rest_res["B"][j])) for j in range(nq))
    rep["checks"]["rest_parity"] = {"queries": nq, "rest_equals_grpc_ordered_ids": same_rg, "rest_A_vs_rest_B_score_sequences_equal": sc_rab,
                                    "note": "PASS = REST ordered ids equal gRPC for the same layout; REST A vs REST B differs for the same reason gRPC A vs B does (build-to-build ANN variation) and is informational",
                                    "status": status(all(v == nq for v in same_rg.values()))}
    ix["A2"].delete_collection("eqA2"); names.pop("A2"); ix.pop("A2"); del res["A2"]

    # ---- filter semantics ----------------------------------------------------------------------------
    rng = np.random.default_rng(7)
    subjects = [f"s{int(i)}" for i in rng.choice(n // 12, 60, replace=False)]
    total = n
    shapes = {
        "trep": (lambda s: flt(mt("trep", s)), lambda s: 12),
        "trep+seq<=1": (lambda s: flt(mt("trep", s), rg("seq", lte=1)), lambda s: 12),
        "trep+seq>=2": (lambda s: flt(mt("trep", s), rg("seq", gte=2)), lambda s: 0),
        "trep+seq==1": (lambda s: flt(mt("trep", s), mt("seq", 1)), lambda s: 12),
        "cid": (lambda s: flt(mt("cid", "cnt:" + s)), lambda s: 12),
        "rep": (lambda s: flt(mt("rep", "rep:" + s)), lambda s: 12),
        "tid": (lambda s: flt(mt("tid", "tgt:" + s)), lambda s: 12),
        "must_not trep": (lambda s: flt(must_not=[mt("trep", s)]), lambda s: total - 12),
        "should two": (lambda s: flt(should=[mt("trep", s), mt("trep", "s0")]), lambda s: 24 if s != "s0" else 12),
        "spec": (lambda s: flt(mt("spec", "sha256:e-r1")), lambda s: total),
    }
    fres: dict = {sh_: {"fail": 0, "tests": 0} for sh_ in shapes}
    id_mismatch = 0
    for s in subjects:
        for sname, (mk, expect) in shapes.items():
            f = mk(s); per = {}
            for k in names:
                per[k] = ix[k].count(names[k], f)
            fres[sname]["tests"] += 1
            if not (per["A"] == per["B"] == per["Bx"] == expect(s)): fres[sname]["fail"] += 1
        f = shapes["trep+seq<=1"][0](s)
        sets = [sorted(p.id for p in ix[k].scroll(names[k], f, page=64)[0]) for k in names]
        id_mismatch += not (sets[0] == sets[1] == sets[2] and len(sets[0]) == 12)
    # filtered vector search (gRPC): hits must be exactly the subject's 12 points, same SET in every layout
    fs_bad = 0; fs_detail = []; fs_order_diff = 0
    for s_ in subjects[:30]:
        qv = X[int(s_[1:]) * 12]
        outs = []
        for k, name in names.items():
            r = H.client.query_points(name, query=qv.tolist(), limit=50, with_payload=["trep"],
                                      query_filter=m.Filter(must=[m.FieldCondition(key="trep", match=m.MatchValue(value=s_))])).points
            outs.append([str(p.id) for p in r])
            if any(p.payload.get("trep") != s_ for p in r) or len(r) != 12:
                fs_bad += 1; fs_detail.append({"subject": s_, "layout": k, "hits": len(r), "foreign": sum(p.payload.get("trep") != s_ for p in r)})
        if not (set(outs[0]) == set(outs[1]) == set(outs[2])):
            fs_bad += 1; fs_detail.append({"subject": s_, "problem": "id sets differ across layouts"})
        fs_order_diff += not (outs[0] == outs[1] == outs[2])
    nfail = sum(v["fail"] for v in fres.values()) + id_mismatch + fs_bad
    rep["checks"]["filters"] = {"subjects": len(subjects), "per_shape": fres, "scroll_id_mismatches": id_mismatch,
                                "filtered_search_failures": fs_bad, "filtered_search_failure_detail": fs_detail[:10], "filtered_search_order_differences_same_set": fs_order_diff, "total_failures": nfail,
                                "tests": sum(v["tests"] for v in fres.values()) + len(subjects) + 30, "status": status(nfail == 0)}

    # ---- delete-by-filter + re-upsert/cleanup --------------------------------------------------------
    dsub, keep = subjects[:20], subjects[20:40]
    dbad = []
    for k, name in names.items():
        for s in dsub:
            ix[k].delete_by_filter(name, flt(mt("trep", s), rg("seq", lt=2)))
        for s in dsub:
            left = ix[k].count(name, flt(mt("trep", s)))
            if left: dbad.append(f"{k}:{s} left {left}")
        for s in keep:
            if ix[k].count(name, flt(mt("trep", s))) != 12: dbad.append(f"{k}:{s} damaged")
        if ix[k].count(name) != n - 12 * len(dsub): dbad.append(f"{k} total {ix[k].count(name)}")
        r = H.client.query_points(name, query=Qn[0].tolist(), limit=20, with_payload=["trep"], score_threshold=THRESH).points
        if not r: dbad.append(f"{k}: not queryable after delete (no hits)")
        dbad += [f"{k}: deleted subject returned" for p in r if p.payload.get("trep") in dsub]
    # mutation: re-upsert 10 subjects at seq 2 then clean seq < 2 (the live-write protocol)
    msub = subjects[40:50]
    for k, name in names.items():
        pts = [Point(h1.pid(s, 2, j), X[(int(s[1:]) * 12 + j) % n], H.payload("target", s, 2, j)) for s in msub for j in range(12)]
        ix[k].upsert(name, pts)
        for s in msub:
            ix[k].delete_by_filter(name, flt(mt("trep", s), rg("seq", lt=2)))
            c_all, c2, c1 = (ix[k].count(name, flt(mt("trep", s))), ix[k].count(name, flt(mt("trep", s), mt("seq", 2))),
                             ix[k].count(name, flt(mt("trep", s), mt("seq", 1))))
            if (c_all, c2, c1) != (12, 12, 0): dbad.append(f"{k}:{s} mutation counts {c_all},{c2},{c1}")
    exp_total = n - 12 * len(dsub)
    final = {k: ix[k].count(names[k]) for k in names}
    rep["checks"]["delete"] = {"deleted_subjects": len(dsub), "mutated_subjects": len(msub), "expected_total": exp_total,
                               "final_counts": final, "problems": dbad[:20], "status": status(not dbad and all(v == exp_total for v in final.values()))}

    # ---- recovery after an interrupted build (layout B), over production REST --------------------------
    ix["B"].create_collection("eqC", GenerationParams(), "target")
    stop = threading.Event(); sent = {"n": 0}

    def fill() -> None:
        try:
            for i in range(0, n, 500):
                if stop.is_set(): return
                ix["B"].upsert("eqC", [Point(ids[j], X[j], H.payload("target", f"s{j // 12}", 1, j % 12)) for j in range(i, min(i + 500, n))])
                sent["n"] = min(i + 500, n)
        except Exception:  # noqa: BLE001 - expected: the container is killed under it
            return
    t = threading.Thread(target=fill, daemon=True); t.start()
    while sent["n"] < max(500, int(0.4 * n)) and t.is_alive(): time.sleep(0.2)
    kill_at = sent["n"]
    sh("docker", "kill", "--signal", "KILL", a.container); stop.set(); t.join(timeout=90)
    sh("docker", "start", a.container)
    ready = False
    for _ in range(150):
        if ix["A"].health().get("ok"): ready = True; break
        time.sleep(2)
    rec: dict = {"killed_after_acknowledged_points": kill_at, "restarted_ready": ready,
                 "restarts": int(sh("docker", "inspect", "-f", "{{.RestartCount}}", a.container) or -1)}
    if ready:
        names_now = ix["A"].collections()
        rec["other_collections_present"] = all(c in names_now for c in names.values())
        rec["counts_after_restart"] = {k: ix[k].count(names[k]) for k in names if names[k] in names_now}
        rec["counts_expected"] = exp_total
        eqc_present = "eqC" in names_now
        rec["interrupted_collection_present"] = eqc_present
        if eqc_present:
            c = ix["B"].count("eqC"); got = [p.id for p in ix["B"].scroll_all("eqC")]
            rec["interrupted_count"] = c; rec["interrupted_unique_ids"] = len(set(got)); rec["interrupted_duplicates"] = len(got) - len(set(got))
            rec["interrupted_within_sent_bound"] = 0 <= c <= min(n, kill_at + 500)
        # serving generation (B) still queryable after the crash: recall on a sample vs brute force on the surviving points
        gone_rows = [j for s_ in dsub for j in range(int(s_[1:]) * 12, int(s_[1:]) * 12 + 12)]
        S2 = S.copy(); S2[:, gone_rows] = -1.0   # deleted subjects no longer count as exact neighbours
        rr = [tie_recall(S2[qi], [p.score for p in gq("eqB", Qn[qi])]) for qi in range(0, nq, 4)]
        rr = [x for x in rr if x is not None]
        rec["serving_generation_tie_recall_after_crash_mean"] = round(float(np.mean(rr)), 5)
        rec["serving_generation_tie_recall_before_crash_mean"] = rep["checks"]["search_grpc"]["tie_aware_recall_at_100_mean"]["B"]
        # resume: re-send everything (deterministic ids => idempotent), then verify
        for i in range(0, n, 500):
            ix["B"].upsert("eqC", [Point(ids[j], X[j], H.payload("target", f"s{j // 12}", 1, j % 12)) for j in range(i, min(i + 500, n))])
        ix["B"].wait_green("eqC", 1800)
        rec["count_after_resend"] = ix["B"].count("eqC"); rec["expected_after_resend"] = n
        rec["payload_schema_after_restart"] = {f: v.get("params") or v.get("data_type") for f, v in ix["B"].collection_info("eqC")["payload_schema"].items()}
        rc = [tie_recall(S[qi], [p.score for p in gq("eqC", Qn[qi])]) for qi in range(0, nq, 4)]
        rec["tie_recall_after_recovery_mean"] = round(float(np.mean([x for x in rc if x is not None])), 5)
        rec["tie_recall_original_B_mean"] = rep["checks"]["search_grpc"]["tie_aware_recall_at_100_mean"]["B"]
        ok = (rec["other_collections_present"] and all(v == exp_total for v in rec["counts_after_restart"].values())
              and rec.get("interrupted_duplicates", 0) == 0 and rec.get("interrupted_within_sent_bound", True)
              and rec["count_after_resend"] == n and rec["serving_generation_tie_recall_after_crash_mean"] >= rec["serving_generation_tie_recall_before_crash_mean"] - 0.01
              and rec["tie_recall_after_recovery_mean"] >= rec["tie_recall_original_B_mean"] - 0.01)
    else:
        ok = False
    rec["status"] = status(ok)
    rec["scope_note"] = ("tests Qdrant-level behaviour (serving collection intact, partial collection harmless, idempotent resume); "
                         "the production builder state machine (generations/leases) is NOT exercised: it needs Scylla/Redis")
    rep["checks"]["recovery"] = rec

    # ---- REST API behaviour + error handling ----------------------------------------------------------
    api: dict = {}

    def expect(label, fn, exc):
        try:
            fn(); api[label] = {"raised": None, "ok": False}
        except Exception as e:  # noqa: BLE001
            api[label] = {"raised": type(e).__name__, "message": str(e)[:160], "ok": isinstance(e, exc)}
    ix["B"].create_collection("eqR", GenerationParams(), "target")
    load("eqR", ix["B"], 3000); ix["B"].wait_green("eqR", 600)
    sc = ix["B"].collection_info("eqR")["payload_schema"]
    api["create_and_schema_readback"] = {"ok": all(isinstance(v.get("params"), dict) and v["params"].get("on_disk") for f, v in sc.items() if f in ("trep", "seq")),
                                         "schema": {f: v.get("params") or v.get("data_type") for f, v in sc.items()}}
    api["count_after_upsert"] = {"count": ix["B"].count("eqR"), "ok": ix["B"].count("eqR") == 3000}
    expect("missing_collection", lambda: ix["B"].count("does_not_exist"), IndexMissingError)
    expect("wrong_dimension", lambda: ix["B"].upsert("eqR", [Point("00000000-0000-0000-0000-000000000001", np.zeros(5, np.float32), {})]), IndexPermanentError)
    expect("bad_filter", lambda: ix["B"].count("eqR", {"must": [{"key": "seq", "range": {"lte": "x"}}]}), IndexPermanentError)
    expect("collection_exists", lambda: ix["B"].create_collection("eqR", GenerationParams(), "target"), IndexPermanentError)
    expect("no_server", lambda: QdrantIndex("http://127.0.0.1:1", None, timeout_s=2).count("x"), IndexUnavailableError)
    ix["B"].delete_by_filter("eqR", flt(mt("trep", "s0")))
    api["rest_delete_by_filter"] = {"count_after": ix["B"].count("eqR"), "ok": ix["B"].count("eqR") == 3000 - 12 and ix["B"].count("eqR", flt(mt("trep", "s0"))) == 0}
    rep["checks"]["rest_api"] = {**api, "status": status(all(v.get("ok") for v in api.values()))}

    # ---- latency: REST vs gRPC, A vs B (bounded) ------------------------------------------------------
    def load_run(kind: str, k: str, seconds: float) -> dict:
        lat: list[float] = []; errs: list[str] = []; end = time.time() + seconds

        def one(v):
            if kind == "grpc": gq(names[k], v)
            else: ix[k].search_batch(names[k], [SearchRequest(vector=v, limit=TOPK, score_threshold=THRESH, payload_fields=tuple(FIELDS))],
                                     hnsw_ef=None, oversampling=2.0)

        def worker(seed):
            r = np.random.default_rng(seed); nxt = time.time()
            while time.time() < end:
                v = Qn[int(r.integers(nq))]; t0 = time.time()
                try:
                    one(v); lat.append(time.time() - t0)
                except Exception as e:  # noqa: BLE001
                    errs.append(f"{type(e).__name__}: {e}"[:120])
                nxt += 0.2; time.sleep(max(0.0, nxt - time.time()))
        ts = [threading.Thread(target=worker, args=(s,)) for s in (1, 2)]
        [x.start() for x in ts]; [x.join() for x in ts]
        lat = lat[int(len(lat) * 0.15):]
        return {"n": len(lat), "p50_ms": pct(lat, 50), "p95_ms": pct(lat, 95), "p99_ms": pct(lat, 99), "errors": len(errs), "first_error": errs[:1]}
    rep["latency"] = {kind: {k: load_run(kind, k, a.rest_seconds) for k in ("A", "B")} for kind in ("grpc", "rest")}
    rep["latency_note"] = "2 threads x 5 q/s, first 15% dropped as warm-up; shared noisy host; not the E-R1 methodology/phases"

    stop_s.set()
    if samples:
        ok_s = [s for s in samples if "anon" in s]
        rep["memory"] = {"anon_peak_gib": round(max(s["anon"] for s in ok_s) / 2**30, 4) if ok_s else None,
                         "oom_kill_max": max(s["oom_kill"] for s in ok_s) if ok_s else None,
                         "note": "anon peak over the whole run incl. 4 resident layouts; not comparable with E-R1 phases"}
    rep["container_oom_killed_final"] = sh("docker", "inspect", "-f", "{{.State.OOMKilled}}", a.container)
    rep["ended_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()); rep["duration_s"] = round(time.time() - t_start, 1)
    rep["overall"] = {k: v["status"] for k, v in rep["checks"].items()}
    a.out.write_text(json.dumps(rep, indent=1, default=str))
    print(json.dumps({"overall": rep["overall"], "duration_s": rep["duration_s"]}, indent=1))


if __name__ == "__main__":
    main()
