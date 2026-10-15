"""Operator CLI (design §18) driven in-process against the real stack."""

from __future__ import annotations

import json
import os

import pytest

from fingerprinter import cli
from fingerprinter.publish import INTAKE, TARGET_REGISTERED
from tests.v2.conftest import ensure_group, integration, settings_env
from tests.v2.helpers import request_env, xadd_request

pytestmark = integration


@pytest.fixture
def run(stack, monkeypatch, capsys):
    env = settings_env(stack.settings)
    for k in [k for k in os.environ if k.startswith("FINGERPRINTER_")]:
        monkeypatch.delenv(k)
    for k, v in env.items():
        if k.startswith("FINGERPRINTER_"):
            monkeypatch.setenv(k, v)

    def call(*args: str, code: int = 0):
        capsys.readouterr()
        got = cli.main(["--json", "--operator", "op", *args])
        out = capsys.readouterr().out
        assert got == code, out
        try:
            return json.loads(out)
        except ValueError:
            return out

    return call


def test_cli_spec_runtime_and_publisher(run, stack):
    run("migrate")
    specs = run("spec", "list")
    assert specs[0]["default"] and specs[0]["status"] == "active"
    fp16 = run("spec", "add", "--builtin", "dinov2-seg/1-fp16")
    assert fp16["status"] == "gated"
    run("spec", "set-default", fp16["spec_digest"], code=2)  # refused: gated
    run("spec", "enable", fp16["spec_digest"], "--evidence", "/nonexistent.json", code=2)
    run("spec", "deprecate", fp16["spec_digest"], code=2)
    out = run("runtime", "check", "--spec", str(stack.spec.digest), "--device", "cpu")
    assert out["allowed"] is True
    status = run("publisher", "status")
    assert {s["stream"] for s in status} >= {TARGET_REGISTERED}
    run("publisher", "register-consumer", "--stream", TARGET_REGISTERED, "--group", "matcher", code=2)
    ensure_group(stack, TARGET_REGISTERED, "matcher")
    run("publisher", "register-consumer", "--stream", TARGET_REGISTERED, "--group", "matcher")
    assert run("publisher", "republish", "--stream", TARGET_REGISTERED)["republished"] == 0


def test_cli_intake_gap_workflow(run, stack):
    stream = stack.coord.stream(INTAKE)
    run("intake", "status", code=1)  # uninitialized
    run("intake", "init", "--from-start")
    xadd_request(stack.coord.client, stream, request_env(b"x", "https://a.example/x"))
    st = run("intake", "run", "--until-idle")
    assert st["consumed_ordinal"] == 1 and st["healthy"]
    for i in range(5):
        xadd_request(stack.coord.client, stream, request_env(f"y{i}".encode(), f"https://a.example/{i}"))
    stack.coord.client.xtrim(stream, maxlen=1, approximate=False)
    stack.clock.advance(seconds=stack.settings.intake.lease_ttl_s + 1)  # new CLI process = new owner
    st = run("intake", "run", "--until-idle")
    assert st["healthy"] is False
    (gap,) = run("intake", "gap", "list")
    shown = run("intake", "gap", "show", gap["gap_id"])
    assert shown["missing_count"] == 4 and shown["inside_crawler2_retention"]
    run("intake", "gap", "replay-start", gap["gap_id"], "--from", "2026-10-10T00:00:00Z",
        "--to", "2026-10-11T00:00:00Z")
    run("intake", "gap", "replay-done", gap["gap_id"])
    run("intake", "accept-gap", gap["gap_id"], "--reason", "x", code=2)  # not from REPLAY_OBSERVING
    run("intake", "gap", "show", "nope", code=2)
    run("intake", "status", code=1)


def test_cli_encode_target_backfill_reconcile(run, stack, tmp_path):
    ensure_group(stack, "representation.ready.v1")
    media = tmp_path / "ref.bin"
    media.write_bytes(os.urandom(10_000))
    reg = run("target", "register", "--title", "T", "--media", str(media), "--alias", "t2")
    shown = run("target", "show", reg["target_id"])
    assert shown["versions"][0]["state"] == "REGISTERING"
    assert len(run("target", "list")) == 1
    status = run("encode", "status")
    assert status["requests"] == {"PENDING": 1}
    run("encode", "retry", "--content", reg["content_id"])  # nothing FAILED: no-op
    run("target", "retire", reg["target_id"], "--reason", "test")
    assert run("target", "purge", reg["target_id"])["deleted_refs"] == [reg["sha256"]]
    job = run("backfill", "create", "--from-spec", str(stack.spec.digest), "--to-spec",
              str(stack.spec.digest))["job_id"]
    run("backfill", "pause", job)
    run("backfill", "resume", job)
    assert run("backfill", "run", job)["enqueued"] == 0
    assert run("backfill", "status", job)["state"] == "ENQUEUED"
    run("backfill", "cancel", job, code=2)
    rep = run("reconcile", "run", "--once")
    assert "intents" in rep and "sweep" in rep
    assert "repairs" in run("reconcile", "full")
    assert "deleted" in run("reconcile", "sweep", "--grace-s", "0")
    run("spec", "retire", str(stack.spec.digest), code=2)  # default spec
    run("spec", "retire-sweep")
    run("encoder", "recalibrate")


def test_reconcile_run_survives_a_storage_failure_in_one_pass(run, monkeypatch):
    """X-10 (SOAK-1000 run 4): a typed storage failure ends one reconcile pass, not the loop."""
    from fingerprinter.errors import StorageUnavailableError
    from fingerprinter.reconcile import Reconciler

    class Stop(BaseException):  # not an Exception: ends the endless loop past cli's handler
        pass

    real, calls = Reconciler.intent_pass, []

    def flaky(self, **kw):
        calls.append(1)
        if len(calls) == 1:
            raise StorageUnavailableError("ReadFailure: code=1300 (injected)")
        if len(calls) == 2:
            return real(self, **kw)
        raise Stop

    monkeypatch.setattr(Reconciler, "intent_pass", flaky)
    monkeypatch.setattr(cli.time, "sleep", lambda s: None)
    with pytest.raises(Stop):
        cli.main(["--json", "--operator", "op", "reconcile", "run"])
    assert len(calls) == 3  # failed pass, a real pass, then stopped


def test_reconcile_run_once_still_reports_a_storage_failure(run, monkeypatch):
    from fingerprinter.errors import StorageUnavailableError
    from fingerprinter.reconcile import Reconciler

    def broken(self, **kw):
        raise StorageUnavailableError("ReadFailure: code=1300 (injected)")

    monkeypatch.setattr(Reconciler, "intent_pass", broken)
    run("reconcile", "run", "--once", code=2)  # typed failure surfaced, not hidden
