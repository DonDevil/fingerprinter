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
