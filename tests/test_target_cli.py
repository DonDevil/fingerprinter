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

    def __init__(self, device="auto", torch_num_threads=None):
        from embedding.config import PreprocessingConfig, SegmentSamplingConfig

        self.model_id = "dinov2-synthetic"
        self.model_version = "v1"
        self.preprocessing_config = PreprocessingConfig()
        self.segment_sampling_config = SegmentSamplingConfig()
        self.calls = 0

    def embed_video_segments(self, artifact):
        self.calls += 1
        return SimpleNamespace(segments=_SEGMENTS, coarse_vector=_COARSE_VECTOR)


class _FailingFakeDINOv2Engine(_FakeDINOv2Engine):
    def embed_video_segments(self, artifact):
        self.calls += 1
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
