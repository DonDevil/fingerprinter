"""P10 operator commands (design §14), added to ``python -m fingerprinter``.

Tools: ``matcher migrate|run|reconcile``, ``index init|status|rebuild|activate|check|audit|drop``,
``match params-register|status|summary|show|rerun|retry|publish-held``,
``equivalence show|revoke``, ``lowinfo status|refresh``. Every state change is audited.
Exit status is non-zero on refusal; ``match status`` exits 1 when the subject's "no match"
is provisional.
"""

from __future__ import annotations

import argparse
import json
import threading
from pathlib import Path
from typing import Any

TOOLS = ("matcher", "index", "match", "equivalence", "lowinfo")


def add_parsers(tools: Any) -> None:
    m = tools.add_parser("matcher").add_subparsers(dest="cmd", required=True)
    m.add_parser("migrate")
    x = m.add_parser("run")
    x.add_argument("--until-idle", action="store_true")
    x = m.add_parser("reconcile")
    x.add_argument("--once", action="store_true")

    ix = tools.add_parser("index").add_subparsers(dest="cmd", required=True)
    x = ix.add_parser("init")
    x.add_argument("--spec", required=True)
    ix.add_parser("status")
    for name in ("rebuild", "activate", "check", "audit"):
        x = ix.add_parser(name)
        x.add_argument("--spec", required=True)
        x.add_argument("--role", choices=("media", "target"), required=True)
        if name == "rebuild":
            x.add_argument("--promote", action="store_true")
        if name in ("activate", "check", "audit"):
            x.add_argument("--generation", type=int)
    x = ix.add_parser("drop")
    x.add_argument("--spec", required=True)

    mt = tools.add_parser("match").add_subparsers(dest="cmd", required=True)
    x = mt.add_parser("params-register")
    x.add_argument("--version", required=True)
    x.add_argument("--from", dest="source", type=Path, required=True)
    x = mt.add_parser("status")
    x.add_argument("subject_id")
    x.add_argument("--spec", required=True)
    mt.add_parser("summary")
    x = mt.add_parser("show")
    x.add_argument("--content")
    x.add_argument("--target")
    for name in ("rerun", "retry"):
        x = mt.add_parser(name)
        x.add_argument("subject_id")
        x.add_argument("--spec", required=True)
        x.add_argument("--kind", required=True)
        x.add_argument("--raise-cap", type=int)
    x = mt.add_parser("publish-held")
    x.add_argument("--version", required=True)

    eq = tools.add_parser("equivalence").add_subparsers(dest="cmd", required=True)
    x = eq.add_parser("show")
    x.add_argument("content_id")
    x = eq.add_parser("revoke")
    for a in ("a", "b"):
        x.add_argument(a)
    x.add_argument("--spec", required=True)
    x.add_argument("--version", required=True)
    x.add_argument("--reason", required=True)

    li = tools.add_parser("lowinfo").add_subparsers(dest="cmd", required=True)
    for name in ("status", "refresh"):
        li.add_parser(name).add_argument("--spec", required=True)


def _out(obj: Any) -> None:
    print(json.dumps(obj, indent=2, sort_keys=True, default=str))


def dispatch(args: argparse.Namespace, svc: Any, actor: str) -> int:
    from fingerprinter.p10system import P10System

    s = P10System.connect(svc)
    t, c = args.tool, args.cmd
    if t == "matcher":
        if c == "migrate":
            s.migrate()
            _out({"migrated": svc.db.keyspace, "capacity": s.capacity.read()})
            return 0
        if c == "run":
            stop = threading.Event()
            _out(s.matcher_loop(stop, until_idle=args.until_idle))
            return 0
        if c == "reconcile":
            while True:
                rep = s.reconcile_once(force=args.once)
                if args.once:
                    _out(rep)
                    return 0
                threading.Event().wait(s.p.settings.matcher_reconcile.interval_s)
    if t == "index":
        if c == "init":
            s.init_spec(args.spec, actor=actor)
            return 0
        if c == "status":
            _out(s.status())
            return 0
        if c == "rebuild":
            res = s.rebuild(args.spec, args.role, promote=args.promote)
            svc.store.audit(actor, "index-rebuild", {"spec": args.spec, "role": args.role, "state": res.get("state")})
            _out(res)
            return 0 if res.get("state") == "READY" else 2
        if c == "activate":
            ok = s.gens.promote(args.spec, args.role, args.generation)
            svc.store.audit(actor, "index-activate", {"spec": args.spec, "role": args.role, "generation":
                                                      args.generation, "applied": ok})
            return 0 if ok else 2
        if c == "check":
            g = args.generation or s.gens.active(args.spec, args.role, fresh=True).generation
            row = s.gens.row(args.spec, args.role, g)
            out = {"verify": s.checker.verify_generation(args.spec, args.role, g),
                   "manifest_recorded": row.get("manifest_digest"),
                   "manifest_from_artifacts": s.builder.manifest_digest(args.spec, args.role)}
            out["manifest_equal"] = out["manifest_recorded"] == out["manifest_from_artifacts"]
            _out(out)
            return 0 if out["manifest_equal"] and not out["verify"].get("short") else 1
        if c == "audit":
            s.gens.set_cols(args.spec, args.role, args.generation, audit_state="REQUESTED")
            svc.store.audit(actor, "index-audit", vars(args))
            return 0
        if c == "drop":
            _out(s.drop_spec(args.spec, actor=actor))
            return 0
    if t == "match":
        if c == "params-register":
            from fingerprinter.confidence import params_from_tune_eval

            params, cal, status = params_from_tune_eval(json.loads(args.source.read_text()))
            ok = s.techniques.register(args.version, params, calibration=cal, status=status, calibration_id=None
                                       if status != "CALIBRATED" else f"cal-{params.digest()[:12]}", actor=actor,
                                       evidence=str(args.source))
            _out({"registered": ok, "version": args.version, "params_digest": params.digest(), "status": status})
            return 0
        if c == "status":
            from fingerprinter.matching import kinds_of
            from fingerprinter.p10ctx import Subject

            rows = [r for r in svc.db.rows("SELECT * FROM {ks}.match_tasks WHERE subject_id = ?", args.subject_id)
                    if r["spec_digest"] == args.spec]
            if not rows:
                print("refused: no task for that subject")
                return 2
            subject = Subject.from_meta(rows[0]["subject"])
            st = s.recovery.settled(args.subject_id, args.spec, kinds_of(subject, s.p))
            _out(st)
            return 0 if st["status"] == "settled" else 1
        if c == "summary":
            out: dict[str, Any] = {}
            for r in s.tasks.all():
                key = f"{r['kind']}:{r['state']}"
                out[key] = out.get(key, 0) + 1
            out["unpublished"] = sum(1 for r in svc.db.stream("SELECT published_at FROM {ks}.matches")
                                     if r["published_at"] is None)
            _out(out)
            return 0
        if c == "show":
            if args.content:
                _out(svc.db.rows("SELECT * FROM {ks}.matches WHERE content_id = ?", args.content))
            else:
                _out(svc.db.rows("SELECT * FROM {ks}.matches_by_target WHERE target_id = ? ALLOW FILTERING",
                                 args.target))
            return 0
        if c in ("rerun", "retry"):
            from fingerprinter.p10ctx import Subject

            row = s.tasks.row(args.subject_id, args.spec, args.kind)
            if row is None:
                print("refused: no such task")
                return 2
            run = s.tasks.rerun(Subject.from_meta(row["subject"]), args.kind, reason=f"operator_{c}", actor=actor,
                                cap=args.raise_cap)
            _out({"run": run})
            return 0 if run else 2
        if c == "publish-held":
            _out(s.committer.publish_held(args.version, actor=actor))
            return 0
    if t == "equivalence":
        if c == "show":
            _out(svc.db.rows("SELECT * FROM {ks}.equivalence_links WHERE content_id = ?", args.content_id))
            return 0
        if c == "revoke":
            _out({"revoked_rows": s.committer.revoke(args.a, args.b, args.spec, args.version, reason=args.reason,
                                                     actor=actor)})
            return 0
    if t == "lowinfo":
        if c == "status":
            _out({"enabled": s.lowinfo.enabled, "rev": s.lowinfo.current_rev(args.spec),
                  "records": len(s.lowinfo.records(args.spec))})
            return 0
        if c == "refresh":
            s.lowinfo.request(args.spec, reason="operator", full=True)
            _out(s.lowinfo.refresh(args.spec))
            return 0
    return 2
