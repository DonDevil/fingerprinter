"""P10 I-4: level-5 equivalence on the E-SET (validation §8; design §10; P10-D13, D18).

Offline: ``equiv/v1`` on every unordered pair of E-SET items (canonical order
by item name), vectors from ``encode_corpus.py`` (P9's production decode,
preprocessing and model, ``dinov2-seg/2`` FP32). Live (``--live``, a dedicated
``*_m3`` keyspace with ``FINGERPRINTER_MATCHING__EQUIVALENCE_ENABLED=true``):
items ingested through P9's commit protocol, media role built and promoted,
``media_vs_media`` tasks run by the production matcher; ``equivalence_links``
compared with the labels; one link revoked (both orientations REVOKED,
evidence kept) and the pair's tasks re-run — the link must not come back.

Labels (provenance): two *variants* of the same excerpt are equivalent;
everything else (other excerpts, the 30 s shift, the 50 % sub-clip) is not.

    env/bin/python benchmarks/p10-index/eset_eval.py --eset ~/anti_piracy/datasets/p10/e-set \
        --vectors ~/anti_piracy/datasets/p10/vectors/e-set [--live] --out benchmarks/p10-index/results/e-set.json
"""

from __future__ import annotations

import argparse
import itertools
import json
import sys
import threading
import time
from pathlib import Path

import numpy as np

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE.parents[1]))
sys.path.insert(0, str(HERE))

from corpus import confusion  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--eset", type=Path, required=True)
    ap.add_argument("--vectors", type=Path, required=True)
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()
    from fingerprinter.verify import EquivParams, Vectors, VerifierParams, equivalence

    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    params = VerifierParams.from_document(tune["final_parameter_set"]["params"])
    man = json.loads((a.eset / "manifest.json").read_text())
    items = man["items"]
    vec = {}
    for it in items:
        z = np.load(a.vectors / (it["file"].replace("/", "__") + ".npz"))
        vec[it["file"]] = Vectors(z["segment_index"], z["pts_s"], z["vectors"].astype(np.float32))
    by = {it["file"]: it for it in items}

    def label(x: str, y: str) -> bool:
        i, j = by[x], by[y]
        return i["role"] == j["role"] == "variant" and i["excerpt"] == j["excerpt"]

    rows, kinds = [], {}
    t0 = time.time()
    for x, y in itertools.combinations(sorted(vec), 2):
        eq = equivalence(vec[x], vec[y], params)
        rows.append((label(x, y), eq.linked))
        for k in (x, y):
            r = by[k]["role"]
            if r != "variant" and by[x]["excerpt"] == by[y]["excerpt"]:
                kinds.setdefault(r, []).append(eq.linked)
    report = {"benchmark": "P10-ESET", "items": len(items), "excerpts": len({i["excerpt"] for i in items}),
              "variants": man["variants"], "rule": EquivParams().__dict__,
              "verifier_params_digest": params.digest(), "pairs": len(rows),
              "offline": confusion(rows), "offline_seconds": round(time.time() - t0, 1),
              "negatives_same_excerpt_linked": {k: int(sum(v)) for k, v in kinds.items()},
              "segments_per_item": {"min": int(min(len(v) for v in vec.values())),
                                    "max": int(max(len(v) for v in vec.values()))}}
    per_variant: dict[str, list[bool]] = {}
    for x, y in itertools.combinations(sorted(vec), 2):
        if label(x, y):
            linked = equivalence(vec[x], vec[y], params).linked
            for k in (x, y):
                per_variant.setdefault(by[k]["variant"], []).append(linked)
    report["offline_recall_by_variant"] = {k: round(float(np.mean(v)), 4) for k, v in sorted(per_variant.items())}

    if a.live:
        report["live"] = live(a, items, vec, label, by)
    a.out.parent.mkdir(parents=True, exist_ok=True)
    a.out.write_text(json.dumps(report, indent=1, sort_keys=True, default=str))
    print(json.dumps({k: report[k] for k in ("offline", "negatives_same_excerpt_linked")} |
                     {"live": report.get("live", {}).get("summary")}, indent=1, default=str))


def live(a, items, vec, label, by) -> dict:
    from fingerprinter import artifact as art
    from fingerprinter.commit import Artifact
    from fingerprinter.confidence import params_from_tune_eval
    from fingerprinter.config import Settings
    from fingerprinter.coord import uuid7
    from fingerprinter.db import migrate
    from fingerprinter.p10ctx import KIND_EQ, Subject
    from fingerprinter.p10system import P10System
    from fingerprinter.services import Services
    from fingerprinter.spec import dinov2_seg_v2
    from tests.v2.conftest import register_default_spec
    from tests.v2.helpers import request_env

    settings = Settings.from_env()
    if not settings.scylla.keyspace.endswith("_m3") or not settings.matching.equivalence_enabled:
        raise SystemExit("refusing: needs a *_m3 keyspace and FINGERPRINTER_MATCHING__EQUIVALENCE_ENABLED=true")
    svc = Services.connect(settings, role="eset", migrate_schema=True)
    migrate(svc.db)
    svc.objects.ensure_buckets()
    svc.pub.seed()
    spec = dinov2_seg_v2()
    digest = str(spec.digest)
    if svc.store.spec_row(digest) is None:
        register_default_spec(svc.store, spec)
    s = P10System.connect(svc)
    s.migrate()
    s.init_spec(digest, actor="eset")
    tune = json.loads((HERE / "results" / "tune-eval.json").read_text())
    p, cal, status = params_from_tune_eval(tune)
    s.techniques.register("2.1", p, calibration=cal, status=status, calibration_id=None, actor="eset", evidence="tune")
    cid = {}
    for it in items:
        env = request_env("sha256:" + it["sha256"], f"https://fixture.local/{it['file']}")
        c = str(env.payload.content.content_id)
        cid[it["file"]] = c
        if svc.store.representation(c, digest) is None:
            svc.requests.upsert_event(env)
            row = svc.requests.claim(c, "default", uuid7())
            v = vec[it["file"]]
            seg = art.Segments(v.segment_index, np.round(v.pts_s * 1000).astype(np.int64), (1, 1000), v.pts_s,
                               v.vectors, 5.0)
            data, sha = art.build(seg, {"eset": it["file"]})
            svc.committer.commit(row, spec, Artifact(data, sha, len(v), float(v.pts_s.max() + 5), "{}", "cuda",
                                                     "float32"))
    distinct = len(set(cid.values())) == len(items)  # P8 level 3: every variant is its own ContentId
    for role in ("target", "media"):
        if s.gens.active(digest, role, fresh=True) is None:
            s.rebuild(digest, role, promote=True, wait_boundary=False)

    def drive() -> None:
        for _ in range(40):
            s.reconcile_once(force=True)
            s.matcher_loop(threading.Event(), until_idle=True)
            if all(t["state"] in ("DONE", "INCOMPLETE", "FAILED") and t.get("done_run") == t["run"]
                   for t in s.tasks.all()):
                return
            time.sleep(5)

    drive()
    from fingerprinter.verify import EQUIV_RULES, equiv_link_version

    rule = settings.matching.equivalence_rule
    ver = equiv_link_version("2.1", rule)   # the link key of the configured rule (v1: "2.1")
    links = {(r["content_id"], r["other_content_id"]): r for r in svc.db.stream("SELECT * FROM {ks}.equivalence_links")
             if r["technique_version"] == ver}
    file_of = {v: k for k, v in cid.items()}
    rows = []
    for x, y in itertools.combinations(sorted(vec), 2):
        lx = links.get((cid[x], cid[y]))
        ly = links.get((cid[y], cid[x]))
        linked = bool(lx and lx["state"] == "LINKED")
        both = bool(lx) == bool(ly)
        rows.append((label(x, y), linked, both))
    eq_tasks = [t for t in s.tasks.all() if t["kind"] == KIND_EQ]
    out = {"rule": rule, "rule_digest": EQUIV_RULES[rule].digest(), "link_version": ver,
           "summary": confusion([(lab, lk) for lab, lk, _ in rows]),
           "both_orientations": all(b for _, _, b in rows), "distinct_content_ids": distinct,
           "tasks": {st: sum(1 for t in eq_tasks if t["state"] == st) for st in {t["state"] for t in eq_tasks}},
           "incomplete_reasons": sorted({t.get("incomplete_reason") or "" for t in eq_tasks} - {""})}
    # revoke one true link; re-run both tasks; it must stay REVOKED, evidence kept (§10)
    pos = next(((x, y) for (x, y), r in links.items() if r["state"] == "LINKED"), None)
    if pos:
        x, y = pos
        n = s.committer.revoke(x, y, digest, ver, reason="eset revocation test", actor="eset")
        for c in (x, y):
            t = next(t for t in eq_tasks if json.loads(t["subject"])["content_id"] == c)
            s.tasks.rerun(Subject.from_meta(t["subject"]), KIND_EQ, reason="revocation-check", actor="eset")
        drive()
        after = [svc.db.one("SELECT * FROM {ks}.equivalence_links WHERE content_id = ? AND other_content_id = ? "
                            "AND spec_digest = ? AND technique_name = 'dinov2-seg-temporal' "
                            "AND technique_version = ?",
                            u, v, digest, ver) for u, v in ((x, y), (y, x))]
        out["revocation"] = {"pair": [file_of.get(x), file_of.get(y)], "revoked_rows": n,
                             "states_after_rerun": [r["state"] for r in after],
                             "evidence_kept": all(bool(r["evidence"]) for r in after)}
    svc.close()
    return out


if __name__ == "__main__":
    main()
