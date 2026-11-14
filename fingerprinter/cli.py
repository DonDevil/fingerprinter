"""Operator CLIs (design §18): ``python -m fingerprinter <tool> <command> [--json]``.

``fingerprinter-<tool> <command>`` in the design is ``python -m fingerprinter <tool>
<command>`` here. Every state-changing command writes an audit record
(``audit_log`` or the object's own audit list). Exit status is non-zero on
refusal, and ``intake status`` exits non-zero while any gap is open.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any

from fingerprinter.config import Settings
from fingerprinter.services import Services


def _out(obj: Any, as_json: bool) -> None:
    if as_json:
        print(json.dumps(obj, indent=2, sort_keys=True, default=str))
    else:
        if isinstance(obj, dict):
            for k, v in obj.items():
                print(f"{k}: {v}")
        else:
            print(obj)


def _actor(args: argparse.Namespace) -> str:
    import getpass

    return getattr(args, "operator", None) or getpass.getuser()


def _ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="python -m fingerprinter")
    p.add_argument("--json", action="store_true")
    p.add_argument("--operator")
    tools = p.add_subparsers(dest="tool", required=True)

    tools.add_parser("migrate")

    rt = tools.add_parser("runtime").add_subparsers(dest="cmd", required=True)
    x = rt.add_parser("fingerprint")
    x.add_argument("--device", default="cpu")
    rt.add_parser("init-profile")
    rt.add_parser("admit-cuda")
    x = rt.add_parser("check")
    x.add_argument("--spec", required=True)
    x.add_argument("--device", default="cpu")
    x = rt.add_parser("approve-compat")
    for a in ("--spec", "--profile", "--evidence"):
        x.add_argument(a, required=True)

    sp = tools.add_parser("spec").add_subparsers(dest="cmd", required=True)
    x = sp.add_parser("add")
    x.add_argument("document", nargs="?")
    x.add_argument("--builtin", choices=["dinov2-seg/1", "dinov2-seg/1-fp16", "dinov2-seg/2",
                                         "dinov2-seg/2-fp16"])
    sp.add_parser("list")
    x = sp.add_parser("enable")
    x.add_argument("digest")
    x.add_argument("--evidence", required=True)
    for name in ("set-default", "deprecate", "retire"):
        sp.add_parser(name).add_argument("digest")
    sp.add_parser("retire-sweep")

    it = tools.add_parser("intake").add_subparsers(dest="cmd", required=True)
    x = it.add_parser("init")
    g = x.add_mutually_exclusive_group(required=True)
    g.add_argument("--from-start", action="store_true")
    g.add_argument("--from-now", action="store_true")
    x.add_argument("--reason", default="")
    it.add_parser("status")
    it.add_parser("run").add_argument("--until-idle", action="store_true")
    x = it.add_parser("reinit")
    x.add_argument("--after-reset", action="store_true", required=True)
    x.add_argument("--reason", required=True)
    gap = it.add_parser("gap").add_subparsers(dest="gapcmd", required=True)
    gap.add_parser("list")
    gap.add_parser("show").add_argument("gap_id")
    x = gap.add_parser("replay-start")
    x.add_argument("gap_id")
    x.add_argument("--from", dest="frm", required=True)
    x.add_argument("--to", required=True)
    gap.add_parser("replay-done").add_argument("gap_id")
    x = gap.add_parser("mark-unreplayable")
    x.add_argument("gap_id")
    x.add_argument("--reason", required=True)
    x = it.add_parser("accept-gap")
    x.add_argument("gap_id")
    x.add_argument("--reason", required=True)

    en = tools.add_parser("encode").add_subparsers(dest="cmd", required=True)
    x = en.add_parser("status")
    x.add_argument("--content")
    x = en.add_parser("retry")
    x.add_argument("--content")
    x.add_argument("--spec-key", default="default")
    x.add_argument("--failure")
    x.add_argument("--since")

    ec = tools.add_parser("encoder").add_subparsers(dest="cmd", required=True)
    x = ec.add_parser("run")
    x.add_argument("--device", default=None)
    x.add_argument("--idle-exit-s", type=float, default=None)
    x.add_argument("--max-attempts", type=int, default=None)
    x = ec.add_parser("recalibrate")
    x.add_argument("--device", default="cuda")

    tools.add_parser("scheduler").add_subparsers(dest="cmd", required=True).add_parser("run")

    rc = tools.add_parser("reconcile").add_subparsers(dest="cmd", required=True)
    x = rc.add_parser("run")
    x.add_argument("--once", action="store_true")
    rc.add_parser("full")
    x = rc.add_parser("sweep")
    x.add_argument("--grace-s", type=float, default=None)

    pb = tools.add_parser("publisher").add_subparsers(dest="cmd", required=True)
    pb.add_parser("status")
    x = pb.add_parser("register-consumer")
    x.add_argument("--stream", required=True)
    x.add_argument("--group", required=True)
    x = pb.add_parser("republish")
    x.add_argument("--stream", required=True)

    tg = tools.add_parser("target").add_subparsers(dest="cmd", required=True)
    x = tg.add_parser("register")
    x.add_argument("--title", required=True)
    x.add_argument("--kind", default="movie")
    x.add_argument("--media", required=True)
    x.add_argument("--alias", action="append", default=[])
    x.add_argument("--release-year", type=int)
    x.add_argument("--target-id")
    tg.add_parser("show").add_argument("target_id")
    tg.add_parser("list")
    x = tg.add_parser("retire")
    x.add_argument("target_id")
    x.add_argument("--reason", required=True)
    tg.add_parser("purge").add_argument("target_id")

    bf = tools.add_parser("backfill").add_subparsers(dest="cmd", required=True)
    x = bf.add_parser("create")
    x.add_argument("--from-spec", required=True)
    x.add_argument("--to-spec", required=True)
    x.add_argument("--targets-only", action="store_true")
    x = bf.add_parser("run")
    x.add_argument("job_id")
    x.add_argument("--max-items", type=int)
    for name in ("pause", "resume", "cancel", "status"):
        bf.add_parser(name).add_argument("job_id")
    from fingerprinter import p10cli

    p10cli.add_parsers(tools)
    return p


def main(argv: list[str] | None = None) -> int:  # noqa: PLR0911, PLR0912, PLR0915 - a command table
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    settings = Settings.from_env()
    if args.tool == "runtime" and args.cmd == "fingerprint":
        from fingerprinter.runtime import RuntimeFingerprint

        _out(json.loads(RuntimeFingerprint.capture(args.device).to_json()), True)
        return 0
    svc = Services.connect(settings, role=args.tool, migrate_schema=args.tool == "migrate")
    if settings.metrics_port and getattr(args, "cmd", None) == "run":
        from prometheus_client import start_http_server

        start_http_server(settings.metrics_port)
    actor = _actor(args)
    try:
        return _dispatch(args, svc, actor)
    except Exception as exc:  # noqa: BLE001 - surface typed refusals as exit 2
        if isinstance(exc, (ValueError, LookupError)) or type(exc).__name__.endswith("Error"):
            print(f"refused: {exc}", file=sys.stderr)
            return 2
        raise
    finally:
        svc.close()


def _dispatch(args: argparse.Namespace, svc: Services, actor: str) -> int:  # noqa: PLR0911, PLR0912, PLR0915
    j = args.json
    store = svc.store
    from fingerprinter import p10cli

    if args.tool in p10cli.TOOLS:
        return p10cli.dispatch(args, svc, actor)
    if args.tool == "migrate":
        svc.objects.ensure_buckets()
        svc.pub.seed()
        _out({"migrated": store.db.keyspace}, j)
        return 0
    if args.tool == "runtime":
        from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile
        from fingerprinter.spec import RUNTIME_PROFILE

        if args.cmd == "init-profile":
            import torch

            prints = [RuntimeFingerprint.capture("cpu")]
            if torch.version.cuda:
                prints.append(RuntimeFingerprint.capture("cuda"))
            profile = RuntimeProfile.from_fingerprints(RUNTIME_PROFILE, *prints)
            created = store.put_profile(profile, actor=actor)
            _out({"profile": RUNTIME_PROFILE, "created": created, "variants": sorted(profile.variants),
                  "admitted": ["cpu"]}, j)
            return 0 if created else 2
        if args.cmd == "admit-cuda":
            from fingerprinter.encoder import GOLDEN_CPU, GOLDEN_MIN_COS
            from fingerprinter.model import Embedder, golden_inputs
            import numpy as np

            golden = np.load(GOLDEN_CPU)
            vec = Embedder("cuda", "float32").embed(golden_inputs(len(golden)))
            cos = float((vec * golden).sum(axis=1).min())
            ok = cos >= GOLDEN_MIN_COS["cuda"]
            evidence = {"check": "cuda fp32 vs cpu golden", "min_cos": cos, "threshold": GOLDEN_MIN_COS["cuda"],
                        "pass": ok, "fingerprint": json.loads(RuntimeFingerprint.capture("cuda").to_json())}
            if ok:
                store.admit_variant(RUNTIME_PROFILE, "cuda", actor=actor, evidence=json.dumps(evidence)[:2000])
            _out(evidence, j)
            return 0 if ok else 2
        if args.cmd == "check":
            fp = RuntimeFingerprint.capture(args.device)
            reason = store.runtime_check(args.spec, fp)
            _out({"spec": args.spec, "device": args.device, "allowed": reason is None, "reason": reason}, j)
            return 0 if reason is None else 2
        if args.cmd == "approve-compat":
            store.approve_compat(args.spec, args.profile, actor=actor, evidence=args.evidence)
            store.audit(actor, "approve-compat", vars(args))
            return 0
    if args.tool == "spec":
        from fingerprinter.spec import Spec, builtin
        from fingerprinter.specs import SpecLifecycle

        life = SpecLifecycle(store, svc.objects)
        if args.cmd == "add":
            if args.builtin:
                spec = builtin(args.builtin)
            else:
                spec = Spec.from_json(Path(args.document).read_bytes())
            status = life.add(spec, actor=actor)
            _out({"spec_digest": str(spec.digest), "name": spec.name, "version": spec.version,
                  "status": status}, j)
            return 0
        if args.cmd == "list":
            rows = store.specs()
            default = store.role("default")
            _out([{"spec_digest": r["spec_digest"], "name": r["name"], "version": r["version"],
                   "status": r["status"], "compute_dtype": r["compute_dtype"],
                   "default": bool(default and default["spec_digest"] == r["spec_digest"])} for r in rows], True)
            return 0
        if args.cmd == "enable":
            life.enable(args.digest, evidence=Path(args.evidence), actor=actor)
        elif args.cmd == "set-default":
            life.set_default(args.digest, actor=actor)
        elif args.cmd == "deprecate":
            life.deprecate(args.digest, actor=actor)
        elif args.cmd == "retire":
            _out({"retired_rows": life.retire(args.digest, actor=actor)}, j)
        elif args.cmd == "retire-sweep":
            _out({"deleted_objects": life.retire_sweep()}, j)
        store.audit(actor, f"spec-{args.cmd}", {k: v for k, v in vars(args).items() if k != "func"})
        return 0
    if args.tool == "intake":
        from fingerprinter.intake import Intake, gap_json

        intake = Intake(store, svc.requests)
        if args.cmd == "init":
            _out(intake.init("from-start" if args.from_start else "from-now", actor=actor, reason=args.reason), j)
            return 0
        if args.cmd == "status":
            st = intake.status()
            _out(st, True)
            return 0 if st["healthy"] else 1
        if args.cmd == "run":
            if args.until_idle:  # drain what is in the stream now, then exit (tests, ops)
                deadline = time.monotonic() + 600
                while not intake.acquire():
                    if time.monotonic() > deadline:
                        return 2
                    time.sleep(1)
                while True:
                    result = intake.cycle()
                    if result.halted or result.lost_lease:
                        _out(intake.status(), True)
                        return 1
                    if result.idle:
                        _out(intake.status(), True)
                        return 0
            stop = threading.Event()
            intake.run(stop)
            return 0
        if args.cmd == "reinit":
            intake.reinit_after_reset(actor=actor, reason=args.reason)
            return 0
        if args.cmd == "accept-gap":
            _out(intake.accept(args.gap_id, operator=actor, reason=args.reason), j)
            return 0
        if args.cmd == "gap":
            if args.gapcmd == "list":
                _out([gap_json(g) for g in intake.gaps()], True)
            elif args.gapcmd == "show":
                g = intake.gap(args.gap_id)
                if g is None:
                    return 2
                out = gap_json(g)
                retention = store.settings.intake.crawler2_outbox_retention_s
                out["inside_crawler2_retention"] = bool(
                    g["window_from"] is None
                    or (store.now().timestamp() - g["window_from"].timestamp()) < retention
                )
                _out(out, True)
            elif args.gapcmd == "replay-start":
                intake.replay_start(args.gap_id, _ts(args.frm), _ts(args.to), actor=actor)
            elif args.gapcmd == "replay-done":
                intake.replay_done(args.gap_id, actor=actor)
            elif args.gapcmd == "mark-unreplayable":
                intake.mark_unreplayable(args.gap_id, actor=actor, reason=args.reason)
            return 0
    if args.tool == "encode":
        if args.cmd == "status":
            rows = svc.requests.all()
            if args.content:
                rows = [r for r in rows if r["content_id"] == args.content]
                _out(rows, True)
                return 0
            from collections import Counter

            _out({"requests": dict(Counter(r["state"] for r in rows)),
                  "failures": dict(Counter(r["last_failure"] for r in rows if r["state"] == "FAILED")),
                  "queue_depth": svc.coord.queue_depth()}, True)
            return 0
        if args.cmd == "retry":
            done = []
            for r in svc.requests.all():
                if r["state"] != "FAILED":
                    continue
                if args.content and (r["content_id"] != args.content or r["spec_key"] != args.spec_key):
                    continue
                if args.failure and r["last_failure"] != args.failure:
                    continue
                if args.since and (r["last_failed_at"] is None or r["last_failed_at"].isoformat() < args.since):
                    continue
                if not args.content and not args.failure:
                    continue
                if svc.requests.retry(r["content_id"], r["spec_key"], actor=actor):
                    done.append(r["content_id"])
            _out({"reopened": done}, j)
            return 0
    if args.tool == "encoder":
        from fingerprinter.encoder import Encoder

        if args.cmd == "recalibrate":
            store.db.run("DELETE FROM {ks}.encoder_hosts WHERE host_id = ?", store.settings.host_id)
            store.audit(actor, "recalibrate", {"host_id": store.settings.host_id})
            return 0
        enc = Encoder(svc, device=args.device)
        code = enc.run(max_attempts=args.max_attempts, idle_exit_s=args.idle_exit_s, install_signals=True)
        _out(getattr(enc, "last_report", {"exit_code": code}), True)
        return code
    if args.tool == "scheduler":
        while True:
            svc.requests.schedule()
            time.sleep(store.settings.encoder.poll_interval_s)
    if args.tool == "reconcile":
        from fingerprinter.reconcile import summarize

        rec = svc.reconciler
        if args.cmd == "full":
            _out(summarize(rec.full_pass()), True)
            return 0
        if args.cmd == "sweep":
            _out(rec.sweep(grace_s=args.grace_s), True)
            return 0
        from fingerprinter.errors import StorageUnavailableError

        last_full = 0.0
        while True:
            try:
                report = {"intents": summarize(rec.intent_pass()), "sweep": rec.sweep()}
                if time.monotonic() - last_full > store.settings.reconciler.full_pass_interval_s:
                    report["full"] = summarize(rec.full_pass())
                    last_full = time.monotonic()
            except StorageUnavailableError as exc:
                if args.once:
                    raise
                # X-10: a transient storage failure ends this pass only; passes are
                # idempotent and the next one resolves what this one could not
                logging.getLogger("fingerprinter.reconcile").error("reconcile_pass_storage_unavailable %s", exc)
                time.sleep(store.settings.reconciler.interval_s)
                continue
            if args.once:
                _out(report, True)
                return 0
            time.sleep(store.settings.reconciler.interval_s)
    if args.tool == "publisher":
        from fingerprinter.targets import TargetManager

        if args.cmd == "status":
            _out(svc.pub.status(), True)
            return 0
        if args.cmd == "register-consumer":
            svc.pub.register_consumer(args.stream, args.group, actor=actor)
            return 0
        if args.cmd == "republish":
            from fingerprinter.errors import ConfigError

            try:
                republished = TargetManager(svc).republish(args.stream)
            except ConfigError as exc:  # a stream that is not a target stream (P10 O-6)
                print(f"error: {exc}", file=sys.stderr)
                return 2
            _out({"republished": republished}, j)
            store.audit(actor, "republish", {"stream": args.stream})
            return 0
    if args.tool == "target":
        from fingerprinter.targets import TargetManager

        tm = TargetManager(svc).install()
        if args.cmd == "register":
            reg = tm.register(Path(args.media), title=args.title, kind=args.kind, aliases=tuple(args.alias),
                              release_year=args.release_year, target_id=args.target_id, actor=actor)
            store.audit(actor, "target-register", reg.__dict__)
            _out(reg.__dict__, True)
            return 0
        if args.cmd == "show":
            _out(tm.show(args.target_id), True)
            return 0
        if args.cmd == "list":
            _out([{k: v for k, v in t.items() if not k.endswith("envelope") and k != "audit"} for t in tm.list()], True)
            return 0
        if args.cmd == "retire":
            _out({"retired_versions": tm.retire(args.target_id, reason=args.reason, actor=actor)}, j)
            return 0
        if args.cmd == "purge":
            _out({"deleted_refs": tm.purge(args.target_id, actor=actor)}, j)
            return 0
    if args.tool == "backfill":
        from fingerprinter.backfill import Backfill

        bf = Backfill(store, svc.requests)
        if args.cmd == "create":
            _out({"job_id": bf.create(args.from_spec, args.to_spec, actor=actor, targets_only=args.targets_only)}, j)
            return 0
        if args.cmd == "run":
            _out(bf.run(args.job_id, actor=actor, max_items=args.max_items), True)
            return 0
        if args.cmd == "status":
            st = bf.status(args.job_id)
            _out(st, True)
            return 0
        getattr(bf, args.cmd)(args.job_id, actor=actor)
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
