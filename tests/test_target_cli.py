"""Target-management design doc — target/cli.py: thin-client behavior,
exit codes, human/--json output, and reindex.

`target.cli.main(argv)` is called in-process (not via subprocess) for
speed; it reads its own wiring from the environment on every call
(REDIS_URL / TARGET_CACHE_PATH), so `monkeypatch.setenv` per test is
sufficient -- no module reload needed.

The `build` subcommand tests below monkeypatch
`embedding.dinov2_engine.DINOv2EmbeddingEngine` with a synthetic engine --
`target.cli._cmd_build` imports that name lazily, inside the handler, so
patching the module attribute before calling `main(["build", ...])` is
sufficient; no real model/ffmpeg is ever exercised here
(tests/test_embedding_lazy_import.py separately proves every *other*
subcommand still never imports torch).
"""
import json
import logging
import os
from types import SimpleNamespace

import pytest

from embedding.errors import UnsupportedMediaError
from embedding.result import SegmentEmbedding
from target.cli import main


@pytest.fixture(autouse=True)
def cli_env(monkeypatch, tmp_path, redis_client):
    redis_url = os.environ.get("FINGERPRINTER_TEST_REDIS_URL", "redis://localhost:6379/15")
    monkeypatch.setenv("REDIS_URL", redis_url)
    monkeypatch.setenv("TARGET_CACHE_PATH", str(tmp_path / "target_cache"))
    monkeypatch.delenv("SHARED_ARTIFACT_STORE_PATH", raising=False)
    yield


@pytest.fixture(autouse=True)
def _restore_root_logging():
    """`target.cli.main()` calls `_configure_logging()`, which -- like
    `worker/observability.py:configure_json_logging` -- installs a handler
    and level on the *root* logger (by design, so `--debug` covers every
    `logger.debug()` call anywhere in the process, not just this module).
    In a real process that's a one-time startup cost; in this shared test
    process it would otherwise leak the root logger's level/handlers into
    unrelated test modules. Restore whatever was there before each test."""
    root = logging.getLogger()
    original_level = root.level
    original_handlers = list(root.handlers)
    yield
    root.setLevel(original_level)
    root.handlers[:] = original_handlers


def _write(tmp_path, name, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return path


def test_cli_add_human_output(tmp_path, capsys):
    media = _write(tmp_path, "movie.mp4", b"hello world")
    exit_code = main(["add", str(media), "--id", "blast", "--version", "v1", "--metadata", "genre=action"])

    assert exit_code == 0
    out = capsys.readouterr().out
    assert "target_id: blast" in out
    assert "target_version: v1" in out
    assert '"genre": "action"' in out


def test_cli_add_json_output(tmp_path, capsys):
    media = _write(tmp_path, "movie.mp4", b"hello world")
    exit_code = main(["add", str(media), "--id", "blast", "--version", "v1", "--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["target_id"] == "blast"
    assert payload["status"] == "ok"


def test_cli_list_json(tmp_path, capsys):
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["list", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert [p["target_id"] for p in payload] == ["blast"]


def test_cli_list_human_output(tmp_path, capsys):
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    main(["list"])
    out = capsys.readouterr().out
    assert "blast\tv1\t" in out


def test_cli_get_existing(tmp_path, capsys):
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["get", "blast", "--version", "v1"])
    assert exit_code == 0
    assert "target_id: blast" in capsys.readouterr().out


def test_cli_get_missing_exits_1(capsys):
    exit_code = main(["get", "nope", "--version", "v1"])
    assert exit_code == 1
    assert "not found" in capsys.readouterr().err


def test_cli_update_metadata(tmp_path, capsys):
    main(
        [
            "add",
            str(_write(tmp_path, "a.mp4", b"a")),
            "--id",
            "blast",
            "--version",
            "v1",
            "--metadata",
            "genre=action",
        ]
    )
    capsys.readouterr()

    exit_code = main(["update-metadata", "blast", "--version", "v1", "--set", "region=IN", "--unset", "genre"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert '"region": "IN"' in out
    assert "genre" not in out


def test_cli_delete(tmp_path, capsys):
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["delete", "blast", "--version", "v1", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"status": "deleted", "target_id": "blast", "target_version": "v1"}

    assert main(["get", "blast", "--version", "v1"]) == 1


_SEGMENTS = (
    SegmentEmbedding(segment_index=0, start_time=0.0, end_time=5.0, vector=(0.1, 0.2, 0.3)),
    SegmentEmbedding(segment_index=1, start_time=5.0, end_time=10.0, vector=(0.4, 0.5, 0.6)),
)
_COARSE_VECTOR = (0.4, 0.5, 0.6)


class _FakeDINOv2Engine:
    """Stands in for `DINOv2EmbeddingEngine` -- see module docstring."""

    #: every instance constructed, in order -- lets a test recover the
    #: instance `_cmd_build` actually built and inspect what it saw, since
    #: the CLI constructs the engine internally (see `instances[-1]` below).
    instances = []

    def __init__(self, device="auto", torch_num_threads=None):
        from embedding.config import PreprocessingConfig, SegmentSamplingConfig

        type(self).instances.append(self)
        self.model_id = "dinov2-synthetic"
        self.model_version = "v1"
        self.model_revision = "synthetic-revision"
        self.device = device
        self.torch_num_threads = torch_num_threads
        self.model_load_duration_s = 0.0
        self.preprocessing_config = PreprocessingConfig()
        self.segment_sampling_config = SegmentSamplingConfig()
        self.calls = 0
        self.timeouts_seen = []

    def embed_video_segments(self, artifact, on_frame=None, timeout="unset"):
        self.calls += 1
        self.timeouts_seen.append(timeout)
        if on_frame is not None:
            for index in range(1, len(_SEGMENTS) + 1):
                on_frame(index, len(_SEGMENTS))
        return SimpleNamespace(segments=_SEGMENTS, coarse_vector=_COARSE_VECTOR)


class _FailingFakeDINOv2Engine(_FakeDINOv2Engine):
    def embed_video_segments(self, artifact, on_frame=None, timeout="unset"):
        self.calls += 1
        self.timeouts_seen.append(timeout)
        raise UnsupportedMediaError("ffmpeg timed out extracting segment frames")


def test_cli_build_success_json(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json"])

    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "built"
    assert payload["target_id"] == "blast"
    assert payload["target_version"] == "v1"
    assert payload["segment_count"] == 2


def test_cli_build_uses_unbounded_ffmpeg_timeout(tmp_path, capsys, monkeypatch):
    """`target.cli build` is explicit, operator-triggered preprocessing
    (unlike the worker's runtime/lazy build-on-miss path) and therefore
    must request `timeout=None` -- no ffmpeg subprocess timeout at all --
    from `embed_video_segments`, not `embed_video_segments`'s own bounded
    default."""
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)
    _FakeDINOv2Engine.instances.clear()
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json"])

    assert exit_code == 0
    assert _FakeDINOv2Engine.instances[-1].timeouts_seen == [None]


def test_cli_build_cache_hit_does_not_rerun_ffmpeg_or_embedding(tmp_path, capsys, monkeypatch):
    """Cache-hit behavior must stay unchanged by the timeout-policy change:
    a second `build` against an already-built target reports
    `already_built` and never calls `embed_video_segments` again."""
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)
    _FakeDINOv2Engine.instances.clear()
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    assert main(["build", "blast", "--version", "v1", "--json"]) == 0
    assert _FakeDINOv2Engine.instances[-1].calls == 1
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "already_built"
    assert _FakeDINOv2Engine.instances[-1].calls == 0  # a fresh engine instance, never invoked


def test_cli_build_debug_emits_diagnostics_and_normal_mode_stays_silent(tmp_path, capsys, monkeypatch):
    """Observability audit, "Debug/Verbose Mode": `--debug` adds cache
    status, engine info, and per-frame progress; the default mode gets
    none of it. Both runs build against a fresh target (own tmp_path) so
    the second isn't just observing a cache hit from the first."""
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json", "--debug"])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert '"status": "built"' in captured.out  # normal --json output still on stdout, unaffected
    assert "DEBUG: target blast/v1: compatible segment embedding not cached, build required" in captured.err
    assert "DEBUG: engine ready: model=dinov2-synthetic" in captured.err
    assert "DEBUG: embedding blast/v1: frame 2/2 (100.0%)" in captured.err
    assert "DEBUG: target blast/v1: resolved 2 segment(s)" in captured.err

    main(["add", str(_write(tmp_path, "b.mp4", b"b")), "--id", "blast2", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["build", "blast2", "--version", "v1", "--json"])

    assert exit_code == 0
    captured = capsys.readouterr()
    assert captured.err == ""  # no --debug: no diagnostic output at all


def test_configure_logging_pins_known_noisy_third_party_loggers_to_warning():
    """redis-py logs a harmless DEBUG line ("Failed to enable maintenance
    notifications: unknown subcommand 'MAINT_NOTIFICATIONS'") when talking
    to a Redis server that predates that optional feature -- not an error,
    and not actionable by this project; PIL logs one line per PNG chunk
    while decoding extracted frames. `--debug` must surface this project's
    own diagnostics without also sweeping in third-party library-internal
    chatter. Tested directly against `_configure_logging` (rather than
    through a real Redis connection) so it doesn't depend on whether the
    test Redis server happens to support MAINT_NOTIFICATIONS."""
    from target.cli import _configure_logging

    _configure_logging(debug=True)

    assert logging.getLogger().getEffectiveLevel() == logging.DEBUG
    assert logging.getLogger("redis").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("redis.connection").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("PIL").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("PIL.PngImagePlugin").getEffectiveLevel() == logging.WARNING
    assert logging.getLogger("target.cli").getEffectiveLevel() == logging.DEBUG  # this project's own loggers


def test_cli_build_is_idempotent_second_run_reports_already_built(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    assert main(["build", "blast", "--version", "v1", "--json"]) == 0
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json"])
    assert exit_code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["status"] == "already_built"


def test_cli_build_target_not_found_exits_1(monkeypatch, capsys):
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FakeDINOv2Engine)

    exit_code = main(["build", "nope", "--version", "v1", "--json"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "TargetNotFoundError"


def test_cli_build_media_failure_exits_1_and_reports_error_type(tmp_path, capsys, monkeypatch):
    monkeypatch.setattr("embedding.dinov2_engine.DINOv2EmbeddingEngine", _FailingFakeDINOv2Engine)
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()

    exit_code = main(["build", "blast", "--version", "v1", "--json"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "UnsupportedMediaError"
    assert "ffmpeg timed out" in payload["message"]


def test_cli_service_error_exit_code_and_stderr(tmp_path, capsys):
    media = _write(tmp_path, "a.mp4", b"a")
    exit_code = main(["add", str(media), "--id", "bad:id", "--version", "v1"])

    assert exit_code == 1
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "TargetValidationError" in captured.err


def test_cli_service_error_json_mode_goes_to_stdout_as_json(tmp_path, capsys):
    media = _write(tmp_path, "a.mp4", b"a")
    exit_code = main(["add", str(media), "--id", "bad:id", "--version", "v1", "--json"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload["error"] == "TargetValidationError"


def test_cli_argparse_usage_error_exits_2(capsys):
    with pytest.raises(SystemExit) as exc_info:
        main(["add", "/some/path", "--id", "blast"])  # missing --version
    assert exc_info.value.code == 2


def test_cli_unknown_command_exits_2():
    with pytest.raises(SystemExit) as exc_info:
        main(["bogus-command"])
    assert exc_info.value.code == 2


def test_cli_reindex_dry_run_then_real_then_idempotent(tmp_path, capsys, redis_client):
    main(["add", str(_write(tmp_path, "a.mp4", b"a")), "--id", "blast", "--version", "v1"])
    capsys.readouterr()
    redis_client.delete("fingerprint:target:index")  # simulate pre-migration state

    exit_code = main(["reindex", "--dry-run", "--json"])
    assert exit_code == 0
    dry_run_payload = json.loads(capsys.readouterr().out)
    assert dry_run_payload["dry_run"] is True
    assert dry_run_payload["added"] == [{"target_id": "blast", "target_version": "v1"}]

    exit_code = main(["list", "--json"])
    assert json.loads(capsys.readouterr().out) == []  # dry-run wrote nothing

    exit_code = main(["reindex", "--json"])
    assert exit_code == 0
    real_payload = json.loads(capsys.readouterr().out)
    assert real_payload["added"] == [{"target_id": "blast", "target_version": "v1"}]

    exit_code = main(["reindex", "--json"])
    idempotent_payload = json.loads(capsys.readouterr().out)
    assert idempotent_payload["added"] == []
