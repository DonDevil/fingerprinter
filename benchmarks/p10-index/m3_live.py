"""P10 I-5: the M3 run through the live system (validation §2.2, §4, §5, §6, §7).

1. **Ingest** into a dedicated keyspace through P9's real protocol: the 7
   S-SRC films as targets (``TargetManager.register`` on the real files, so
   content ids are the production ones; the reference representation
   committed by P9's ``Committer`` from the cached ``dinov2-seg/2`` vectors —
   P9 encoded those vectors, so no GPU work is repeated) and every L-PAIRS
   clip (and with ``--soak`` every SOAK-1000 item) as media (``encode.requested``
   envelope with the item's real SHA-256 → claim → commit).
2. **Index**: ``index rebuild`` of both roles (target first) + promotion.
3. **Match**: the production matcher and reconciler until idle (technique 2.1,
   lowinfo disabled, candidate policy of design §7.2).
4. **Evaluate** on the labelled pairs:
   - *deployed*: a pair is positive iff a ``matches`` row exists (directions A or B);
   - *nested*: for each outer fold the parameter set ``tune_eval.py`` selected on
     the other six films, applied by ``verify()`` to the **live candidate
     sets** (a pair not proposed by the live ANN path is a negative) — the
     M3-1 numbers; decisions are a pure function of artifacts, parameters
     and exclusion set (V-1), so this equals running the system per fold;
   - candidate recall of the live ANN path vs the exact-search emulation;
   - direction-B recall against provenance; task outcome counts; bounds.
5. ``--drop-rebuild``: §5 — record decisions and manifests, delete every
   collection, check tasks wait, rebuild, rerun every task, compare.

    . scripts/p10-env.sh
    FINGERPRINTER_SCYLLA__KEYSPACE=fingerprinter_m3 env/bin/python benchmarks/p10-index/m3_live.py \
        --data ~/anti_piracy/datasets/p9 --vectors ~/anti_piracy/datasets/p10/vectors [--soak] [--drop-rebuild] \
        --out benchmarks/p10-index/results/m3-live-lpairs.json
"""

from __future__ import annotations

import argparse
import json
import sys
import threading
import time
from collections import defaultdict
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(HERE))

from corpus import confusion, load_lpairs, load_nsoak  # noqa: E402


def artifact_of(vectors_npz: Path):
    from fingerprinter import artifact as art
    from fingerprinter.commit import Artifact

    z = np.load(vectors_npz)
    v, pts = z["vectors"].astype(np.float32), z["pts_s"].astype(np.float64)
    seg = art.Segments(z["segment_index"].astype(np.int64), np.round(pts * 1000).astype(np.int64), (1, 1000), pts, v,
                       5.0)
    data, sha = art.build(seg, {"source": "cached dinov2-seg/2 vectors (P9 encoder output)", "file": vectors_npz.name})
    return Artifact(data, sha, len(v), float(pts.max() + 5.0) if len(v) else 0.0, "{}", "cuda", "float32")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", type=Path, required=True)
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--soak", action="store_true", help="also ingest SOAK-1000 (N-SOAK, 19x growth)")
    ap.add_argument("--soak-limit", type=int, help="first N SOAK items only (the 10x scale)")
    ap.add_argument("--drop-rebuild", action="store_true")
    ap.add_argument("--rerun-all", action="store_true",
                    help="before matching, re-run every existing task (re-verify with the current code)")
    ap.add_argument("--rerun-targets", action="store_true",
                    help="after matching, re-run every target_vs_media task (direction B at the grown corpus)")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    from fingerprinter.confidence import params_from_tune_eval
    from fingerprinter.config import Settings
    from fingerprinter.coord import uuid7
    from fingerprinter.db import migrate
    from fingerprinter.errors import StorageUnavailableError
    from fingerprinter.p10system import P10System
    from fingerprinter.services import Services
    from fingerprinter.spec import dinov2_seg_v2
    from fingerprinter.targets import TargetManager
    from fingerprinter.verify import VerifierParams, verify
    from tests.v2.conftest import register_default_spec
    from tests.v2.helpers import request_env

    t0 = time.time()
    settings = Settings.from_env()
    if not settings.scylla.keyspace.endswith("_m3"):
        raise SystemExit("refusing: use a dedicated *_m3 keyspace")
    svc = Services.connect(settings, role="m3", migrate_schema=True)
    migrate(svc.db)
    svc.objects.ensure_buckets()
    svc.pub.seed()
    spec = dinov2_seg_v2()
    digest = str(spec.digest)
    if svc.store.spec_row(digest) is None:
        register_default_spec(svc.store, spec)
    tm = TargetManager(svc)
    s = P10System.connect(svc)
    s.migrate()
    s.init_spec(digest, actor="m3")
    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    params, cal, status = params_from_tune_eval(tune)
    s.techniques.register("2.1", params, calibration=cal, status=status, calibration_id=None, actor="m3",
                          evidence="benchmarks/p10-index/results/tune-eval.json")

    lp = load_lpairs(a.data)
    ns = load_nsoak(a.data, a.vectors) if a.soak else None
    lp_items = json.loads((a.data / "l-pairs" / "manifest.json").read_text())["items"]
    manifest = {Path(i["file"]).name: i for i in lp_items}
    soak_manifest = {i["file"]: i for i in json.loads((a.data / "soak-1000" / "manifest.json").read_text())["items"]}

    # --- 1. ingest -------------------------------------------------------------------------
    cid_of: dict[str, str] = {}
    film_target: dict[str, tuple[str, int, str]] = {}
    existing_targets = {t["title"]: t for t in tm.list()}
    for film, fname in lp.target_file.items():
        if film in existing_targets:
            t = existing_targets[film]
            film_target[film] = (t["target_id"], t["version"], t["content_id"])
            continue
        path = (a.data / "l-pairs" / "targets" / fname).resolve()
        for attempt in range(5):  # P9 raises StorageUnavailableError on a transient MinIO overload: retry
            try:
                reg = tm.register(path, title=film, kind="movie", actor="m3")
                break
            except StorageUnavailableError:
                if attempt == 4:
                    raise
                time.sleep(30)
        req = next(r for r in svc.requests.all() if r["content_id"] == reg.content_id)
        if req["state"] != "READY":
            row = svc.requests.claim(reg.content_id, req["spec_key"], uuid7())
            svc.committer.commit(row, spec, artifact_of(a.vectors / "l-pairs" / f"targets__{fname}.npz"))
        film_target[film] = (reg.target_id, reg.version, reg.content_id)
    items: list[tuple[str, Path, str]] = [(n, a.vectors / "l-pairs" / f"clips__{n}.npz", manifest[n]["sha256"])
                                         for n in lp.items]
    if ns is not None:
        soak_items = sorted(ns.items)[: a.soak_limit] if a.soak_limit else sorted(ns.items)
        items += [(n, a.vectors / "soak-1000" / (n.replace("/", "__") + ".npz"), soak_manifest[n]["sha256"])
                  for n in soak_items]
    ingested = 0
    for name, npz, sha in items:
        env = request_env("sha256:" + sha, f"https://fixture.local/{name}")
        cid = str(env.payload.content.content_id)
        cid_of[name] = cid
        if svc.store.representation(cid, digest) is not None:
            continue
        svc.requests.upsert_event(env)
        row = svc.requests.claim(cid, "default", uuid7())
        if row is not None:
            svc.committer.commit(row, spec, artifact_of(npz))
            ingested += 1
    t_ingest = time.time() - t0
    print(json.dumps({"ingested": ingested, "items": len(items), "seconds": round(t_ingest)}), flush=True)

    # --- 2. index ----------------------------------------------------------------------------
    t1 = time.time()
    builds = {}
    for role in ("target", "media"):
        if s.gens.active(digest, role, fresh=True) is None:
            builds[role] = {k: v for k, v in s.rebuild(digest, role, promote=True, wait_boundary=False).items()
                            if k != "failures"}
    t_index = time.time() - t1
    print(json.dumps({"builds": builds, "seconds": round(t_index)}), flush=True)

    # --- 3. match -----------------------------------------------------------------------------
    def drive(limit_s: float = 7200.0) -> dict:
        """Reconciler + matcher rounds until every task's current run has finished (or the limit):
        tasks that wait (backoff, WAITING_INDEX) are waited for, not abandoned."""
        counts: dict[str, int] = defaultdict(int)
        deadline = time.time() + limit_s
        while time.time() < deadline:
            s.reconcile_once(force=True)
            out = s.matcher_loop(threading.Event(), until_idle=True)
            for k, v in out.items():
                counts[k] += v
            pending = [t for t in s.tasks.all()
                       if not (t["state"] == "FAILED" or (t["state"] in ("DONE", "INCOMPLETE")
                                                         and t.get("done_run") == t["run"]))]
            if not pending:
                break
            counts["waits"] += 1
            time.sleep(10)
        return dict(counts)

    t2 = time.time()
    if a.rerun_all:
        from fingerprinter.p10ctx import Subject as TaskSubject

        for t in s.tasks.all():
            s.tasks.rerun(TaskSubject.from_meta(t["subject"]), t["kind"], reason="m3-rerun-all", actor="m3")
    run_counts = drive()
    if a.rerun_targets:
        from fingerprinter.p10ctx import Subject as TaskSubject

        for t in s.tasks.all():
            if t["kind"] == "target_vs_media":
                s.tasks.rerun(TaskSubject.from_meta(t["subject"]), t["kind"], reason="m3-direction-b", actor="m3")
        for k, v in drive().items():
            run_counts[f"rerun_b:{k}"] = v
    t_match = time.time() - t2
    print(json.dumps({"matcher": run_counts, "seconds": round(t_match)}), flush=True)

    # --- 4. evaluate ------------------------------------------------------------------------
    tid_of_film = {f: v[0] for f, v in film_target.items()}
    rows = list(svc.db.stream("SELECT * FROM {ks}.matches"))
    decided = {(r["content_id"], r["target_id"]): r for r in rows}
    tasks = s.tasks.all()
    task_states: dict[str, int] = defaultdict(int)
    for t in tasks:
        task_states[f"{t['kind']}:{t['state']}:{(t.get('incomplete_reason') or '').split('=')[0]}"] += 1
    # live candidate sets (A: per media, the target reps proposed; B: per target, the contents proposed)
    proposed: set[tuple[str, str]] = set()
    trep_film = {}
    for f, (tid, ver, _) in film_target.items():
        trep_film[s.p.target_subject(tid, ver, digest).subject_id] = f
    cands_a = cands_b = 0
    proposed_b: set[tuple[str, str]] = set()
    for t in tasks:
        g, r = t.get("searched_generation"), t.get("searched_run")
        if g is None:
            continue
        cand_rows = svc.db.rows("SELECT * FROM {ks}.match_candidates WHERE subject_id = ? AND spec_digest = ? "
                                "AND kind = ? AND generation = ? AND run = ?", t["subject_id"], digest, t["kind"], g, r)
        subj = json.loads(t["subject"])
        for c in cand_rows:
            other = json.loads(c["other"])
            if t["kind"] == "media_vs_targets":
                proposed.add((subj["content_id"], trep_film.get(other.get("trep"), "?")))
                cands_a += 1
            elif t["kind"] == "target_vs_media":
                proposed.add((other.get("cid"), trep_film.get(subj["subject_id"], "?")))
                proposed_b.add((other.get("cid"), trep_film.get(subj["subject_id"], "?")))
                cands_b += 1
    pairs = [(p, "l-pairs") for p in lp.pairs] + ([(p, "soak") for p in ns.pairs if p.item in cid_of] if ns else [])
    labels = np.array([p.label for p, _ in pairs])

    def dec_deployed(p) -> bool:
        return (cid_of[p.item], tid_of_film[p.target]) in decided

    deployed = np.array([dec_deployed(p) for p, _ in pairs])
    live_prop = np.array([(cid_of[p.item], p.target) in proposed for p, _ in pairs])
    live_b = np.array([(cid_of[p.item], p.target) in proposed_b for p, _ in pairs])
    # nested: per-fold parameters on the live candidate sets
    vec_cache: dict[str, object] = {}

    def vecs(key: str, npz: Path):
        if key not in vec_cache:
            from fingerprinter.verify import Vectors

            z = np.load(npz)
            vec_cache[key] = Vectors(z["segment_index"], z["pts_s"], z["vectors"].astype(np.float32))
        return vec_cache[key]

    npz_of = {n: f for n, f, _ in items}
    nested = np.zeros(len(pairs), dtype=bool)
    for i, (p, _) in enumerate(pairs):
        if not live_prop[i]:
            continue
        fold = tune["outer_folds"][p.fold]
        cfg = fold["selected"]
        fp = VerifierParams(s_seg=cfg["s_seg"], s_mean=round(cfg["s_seg"] + cfg["s_mean_plus"], 6),
                            min_run=cfg["min_run"], phase_tolerance_s=cfg["phase_tolerance_s"], max_gap=cfg["max_gap"],
                            vote_floor=round(cfg["s_seg"] - 0.10, 6), tau_confirmed=fold["tau_c"],
                            tau_probable=fold["tau_p"])
        res = verify(vecs(p.item, npz_of[p.item]),
                     vecs("film:" + p.target, a.vectors / "l-pairs" / f"targets__{lp.target_file[p.target]}.npz"), fp)
        nested[i] = res.verdict == "confirmed"
    corp = np.array([c for _, c in pairs])

    def rep(mask) -> dict:
        return {"deployed_2_1": confusion(list(zip(labels[mask].tolist(), deployed[mask].tolist()))),
                "nested_heldout": confusion(list(zip(labels[mask].tolist(), nested[mask].tolist()))),
                "live_candidate_recall": round(float((live_prop & labels & mask).sum()
                                                     / max((labels & mask).sum(), 1)), 4),
                "direction_b_candidate_recall": round(float((live_b & labels & mask).sum()
                                                            / max((labels & mask).sum(), 1)), 4),
                "pairs": int(mask.sum())}

    exact_ref = tune["candidate_generation"]["candidate_recall_vs_provenance"]
    report = {
        "benchmark": "P10-M3-LIVE", "keyspace": settings.scylla.keyspace, "spec": digest,
        "corpus": {"l-pairs": len(lp.items), "soak": (len(items) - len(lp.items)), "targets": len(film_target),
                   "media_points": sum(int(svc.store.representation(cid_of[n], digest)["segment_count"])
                                       for n, _, _ in items)},
        "seconds": {"ingest": round(t_ingest), "index": round(t_index), "match": round(t_match)},
        "builds": builds, "matcher_outcomes": run_counts, "task_states": dict(task_states),
        "candidates": {"direction_a_rows": cands_a, "direction_b_rows": cands_b,
                       "b_tasks": [json.loads(t.get("ann_stats") or "{}") | {"state": t["state"],
                                                                          "reason": t.get("incomplete_reason")}
                                   for t in tasks if t["kind"] == "target_vs_media"]},
        "results": {"all": rep(np.ones(len(pairs), dtype=bool)), "l-pairs": rep(corp == "l-pairs"),
                    "n-soak": rep(corp == "soak")},
        "exact_emulation_candidate_recall_all": exact_ref,
        "matches_rows": len(rows), "unpublished": sum(1 for r in rows if r["published_at"] is None),
        "events_xadded": svc.coord.client.xlen(svc.coord.stream("match.found.v1")),
        "recovery": dict(zip(("open", "unresolved"), s.recovery.counters(digest), strict=True)),
        "index_status": {role: s.gens.row(digest, role, s.gens.active(digest, role, fresh=True).generation)
                         ["actual_count"] for role in ("target", "media")},
        "capacity": s.capacity.read(),
    }
    # direction B against provenance: every relevant item for each film found by its target task
    b_found = defaultdict(lambda: [0, 0])
    for (p, _), d in zip(pairs, deployed, strict=True):
        if p.label:
            b_found[p.target][1] += 1
            b_found[p.target][0] += int(d)
    report["recall_by_film_deployed"] = {f: f"{x}/{n}" for f, (x, n) in sorted(b_found.items())}

    # --- 5. drop and rebuild (§5) -----------------------------------------------------------------
    if a.drop_rebuild:
        before = {r["match_id"]: (r["verdict"], r["score"], r["alignment"]) for r in rows}
        manifests = {}
        for role in ("target", "media"):
            ptr = s.gens.active(digest, role, fresh=True)
            manifests[role] = s.gens.row(digest, role, ptr.generation)["manifest_digest"]
            s.p.index.delete_collection(ptr.collection)
        probe_t = tasks[0]
        from fingerprinter.p10ctx import Subject

        s.tasks.rerun(Subject.from_meta(probe_t["subject"]), probe_t["kind"], reason="drop-probe", actor="m3")
        s.matcher_loop(threading.Event(), until_idle=True)
        waiting = s.tasks.row(probe_t["subject_id"], digest, probe_t["kind"])["state"]
        t3 = time.time()
        rebuilt = {}
        for role in ("target", "media"):
            res = s.rebuild(digest, role, promote=True, wait_boundary=False)
            ptr = s.gens.active(digest, role, fresh=True)
            rebuilt[role] = {"state": res.get("state"), "promoted": res.get("promoted"),
                             "manifest_equal": s.gens.row(digest, role, ptr.generation)["manifest_digest"]
                             == manifests[role]}
        t_rebuild = time.time() - t3
        for t in s.tasks.all():
            s.tasks.rerun(Subject.from_meta(t["subject"]), t["kind"], reason="rebuild-check", actor="m3")
        drive()
        after = {r["match_id"]: (r["verdict"], r["score"], r["alignment"])
                 for r in svc.db.stream("SELECT * FROM {ks}.matches")}
        cands = list(svc.db.stream("SELECT match_id, reason FROM {ks}.match_candidates"))
        reproposed = {c["match_id"] for c in cands if c.get("reason") == "already_decided"}
        report["drop_rebuild"] = {"task_state_while_lost": waiting, "rebuilt": rebuilt,
                                  "rebuild_seconds": round(t_rebuild), "decisions_identical": after == before,
                                  "decisions_before": len(before), "decisions_after": len(after),
                                  "reproposed_fraction": round(len(set(before) & reproposed) / max(len(before), 1), 4),
                                  "not_reproposed": sorted(set(before) - reproposed)[:20],
                                  "recovery": dict(zip(("open", "unresolved"), s.recovery.counters(digest),
                                                       strict=True))}
    report["wall_seconds"] = round(time.time() - t0)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    print(json.dumps(report["results"], indent=1, default=str))
    svc.close()


if __name__ == "__main__":
    main()
