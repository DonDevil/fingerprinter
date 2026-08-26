"""Target-management design doc — target/cli.py: thin-client behavior,
exit codes, human/--json output, and reindex.

`target.cli.main(argv)` is called in-process (not via subprocess) for
speed; it reads its own wiring from the environment on every call
(REDIS_URL / TARGET_CACHE_PATH), so `monkeypatch.setenv` per test is
sufficient -- no module reload needed.
"""
import json
import os

import pytest

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
