"""Offline model loading: local_files_only must imply zero Hub network traffic.

Regression for the 2026-11-16 Blast test, where huggingface_hub fetched
``/api/agent-harnesses`` (AI-agent detection registry, called while building the
User-Agent) during a ``local_files_only=True`` load.
"""

from __future__ import annotations

import socket

import pytest

pytest.importorskip("huggingface_hub")
pytest.importorskip("transformers")

from huggingface_hub import constants  # noqa: E402
from huggingface_hub.utils import _detect_agent, build_hf_headers  # noqa: E402

from fingerprinter import model as model_mod  # noqa: E402


@pytest.fixture
def blocked_network(monkeypatch, tmp_path):
    attempts: list[object] = []

    def refuse(self, address, *a, **k):
        attempts.append(address)
        raise OSError("network blocked by test")

    monkeypatch.setattr(socket.socket, "connect", refuse)
    monkeypatch.setattr(constants, "HF_HUB_OFFLINE", False)
    monkeypatch.setattr(constants, "AGENT_HARNESSES_PATH", str(tmp_path / ".agent_harnesses.json"))
    monkeypatch.setattr(_detect_agent, "_registry", None)
    return attempts


def _fake_from_pretrained(calls):
    class FakeModel:
        def to(self, device):
            return self

        def eval(self):
            return self

    def fake(*args, **kwargs):
        # What every real hub call does internally when it builds request headers.
        build_hf_headers()
        calls.append(kwargs)
        return FakeModel()

    return fake


def test_local_files_only_makes_no_network_attempt(monkeypatch, blocked_network, tmp_path):
    import transformers

    calls: list[dict] = []
    monkeypatch.setattr(transformers.AutoModel, "from_pretrained", _fake_from_pretrained(calls))
    model_mod.Embedder("cpu", "float32", cache_dir=tmp_path, local_files_only=True)
    assert calls and calls[0]["local_files_only"] is True
    assert blocked_network == []
    # global state restored
    assert constants.HF_HUB_OFFLINE is False


def test_negative_control_online_mode_attempts_registry_fetch(monkeypatch, blocked_network, tmp_path):
    """Proves the blocker detects the original defect (no offline guard => attempt)."""
    import transformers

    monkeypatch.setattr(transformers.AutoModel, "from_pretrained", _fake_from_pretrained([]))
    monkeypatch.delenv("HF_HUB_DISABLE_TELEMETRY", raising=False)
    model_mod.Embedder("cpu", "float32", cache_dir=tmp_path, local_files_only=False)
    assert blocked_network, "expected a blocked connection attempt when not offline"


def test_hub_offline_restores_env(monkeypatch):
    monkeypatch.delenv("HF_HUB_OFFLINE", raising=False)
    with model_mod._hub_offline(True):
        import os

        assert os.environ["HF_HUB_OFFLINE"] == "1"
    import os

    assert "HF_HUB_OFFLINE" not in os.environ


# ---- DINOv2 engine (embedding/dinov2_engine.py), missing files, concurrency ----


def _fake_engine_patches(monkeypatch, calls):
    import transformers

    class FakeModel:
        class config:  # noqa: N801
            hidden_size = 768

        def to(self, device):
            return self

        def eval(self):
            return self

    def fake(*args, **kwargs):
        build_hf_headers()
        calls.append(kwargs)
        return FakeModel()

    monkeypatch.setattr(transformers.AutoModel, "from_pretrained", fake)
    monkeypatch.setattr(transformers.AutoImageProcessor, "from_pretrained", fake)


def test_dinov2_engine_offline_makes_no_network_attempt(monkeypatch, blocked_network):
    pytest.importorskip("torch")
    from embedding import dinov2_engine

    calls: list[dict] = []
    monkeypatch.setattr(dinov2_engine, "AutoModel", __import__("transformers").AutoModel)
    monkeypatch.setattr(dinov2_engine, "AutoImageProcessor", __import__("transformers").AutoImageProcessor)
    _fake_engine_patches(monkeypatch, calls)
    dinov2_engine.DINOv2EmbeddingEngine(device="cpu", local_files_only=True)
    assert len(calls) == 2 and all(c["local_files_only"] is True for c in calls)
    assert blocked_network == []
    assert constants.HF_HUB_OFFLINE is False


def test_dinov2_engine_online_mode_unchanged(monkeypatch, blocked_network):
    """Online behaviour preserved: no offline guard, local_files_only=False passed through."""
    pytest.importorskip("torch")
    from embedding import dinov2_engine

    calls: list[dict] = []
    monkeypatch.setattr(dinov2_engine, "AutoModel", __import__("transformers").AutoModel)
    monkeypatch.setattr(dinov2_engine, "AutoImageProcessor", __import__("transformers").AutoImageProcessor)
    _fake_engine_patches(monkeypatch, calls)
    dinov2_engine.DINOv2EmbeddingEngine(device="cpu", local_files_only=False)
    assert all(c["local_files_only"] is False for c in calls)
    assert blocked_network, "online mode must be allowed to attempt the registry fetch"


def test_dinov2_engine_missing_local_files_fails_clearly_without_network(blocked_network):
    pytest.importorskip("torch")
    from embedding.dinov2_engine import DINOv2EmbeddingEngine
    from embedding.errors import ModelLoadError

    with pytest.raises(ModelLoadError, match="not cached locally"):
        DINOv2EmbeddingEngine(
            model_id="facebook/dinov2-nonexistent-offline-test", model_revision="0" * 40, device="cpu",
            local_files_only=True,
        )
    assert blocked_network == []


def test_embedder_missing_local_files_fails_clearly_without_network(blocked_network, tmp_path):
    with pytest.raises(model_mod.ModelUnavailableError):
        model_mod.Embedder("cpu", "float32", cache_dir=tmp_path / "empty", local_files_only=True)
    assert blocked_network == []
    assert constants.HF_HUB_OFFLINE is False


def test_hub_offline_restores_state_after_exception():
    import os

    before = os.environ.get("HF_HUB_OFFLINE")
    with pytest.raises(RuntimeError):
        with model_mod._hub_offline(True):
            raise RuntimeError("boom")
    assert os.environ.get("HF_HUB_OFFLINE") == before
    assert constants.HF_HUB_OFFLINE is False


def test_hub_offline_serializes_concurrent_loaders(monkeypatch):
    """An online loader must never observe an offline loader's temporary setting (and vice versa)."""
    import threading
    import time

    from fingerprinter.hub_offline import hub_offline

    monkeypatch.setattr(constants, "HF_HUB_OFFLINE", False)
    inside = threading.Event()
    seen: dict[str, bool] = {}

    def offline_loader():
        with hub_offline(True):
            inside.set()
            time.sleep(0.3)
            seen["offline_sees"] = constants.HF_HUB_OFFLINE

    def online_loader():
        inside.wait()
        with hub_offline(True):  # a second offline loader waits for the lock
            seen["second_sees"] = constants.HF_HUB_OFFLINE
        seen["after"] = constants.HF_HUB_OFFLINE

    t = [threading.Thread(target=offline_loader), threading.Thread(target=online_loader)]
    [x.start() for x in t]
    [x.join() for x in t]
    assert seen == {"offline_sees": True, "second_sees": True, "after": False}
