"""P10 test fixtures (validation.md "Test classes and run status").

Integration tests run against the **real** pinned dependencies: Scylla 6.2.3,
Redis and MinIO of crawler2's compose stack, and the fingerprinter-owned dev
Qdrant 1.15.1 (``. scripts/p10-env.sh``; ``RUN_P10_INTEGRATION=1``).
Isolation: keyspace ``fingerprinter_it_p10`` (every partition deleted before
each test), per-test Redis prefixes and MinIO buckets (as P9), and per-test
spec digests, so every Qdrant collection a test creates has a unique name;
the test deletes them afterwards. Unit tests need no services.
"""

from __future__ import annotations

import json
import os
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from fingerprinter.config import Settings
from fingerprinter.vindex import QdrantIndex

INTEGRATION = os.environ.get("RUN_P10_INTEGRATION") == "1"
EVIDENCE_DIR = os.environ.get("P10_EVIDENCE_DIR")


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "p10_integration: needs Scylla/Redis/MinIO and the dev Qdrant")


def need_stack() -> None:
    if not INTEGRATION:
        pytest.skip("needs the stack: . scripts/p10-env.sh")


def record_evidence(name: str, doc: dict[str, Any]) -> None:
    """Write a measured outcome as JSON when ``P10_EVIDENCE_DIR`` is set (validation reports)."""
    if EVIDENCE_DIR:
        path = Path(EVIDENCE_DIR) / f"{name}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(doc, indent=1, sort_keys=True, default=str))


def test_spec_digest() -> str:
    return "sha256:" + uuid.uuid4().hex + uuid.uuid4().hex


test_spec_digest.__test__ = False  # type: ignore[attr-defined]


class TrackingIndex(QdrantIndex):
    """Records the collections a test creates so teardown deletes exactly those."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.created: set[str] = set()

    def create_collection(self, name: str, params: Any, role: str) -> None:
        self.created.add(name)
        super().create_collection(name, params, role)

    def raw(self, method: str, path: str, body: Any = None) -> Any:
        """Direct REST call for test-only setup (e.g. optimizer thresholds)."""
        if method == "PUT" and path.startswith("/collections/") and path.count("/") == 2:
            self.created.add(path.split("/")[2])
        return self._call(method, path, body, timeout_s=120)


_DB = None


def p10_db():
    """The test keyspace with the P9 and P10 schema (connect + migrate once per session)."""
    global _DB
    if _DB is None:
        from fingerprinter.db import Db, migrate
        from fingerprinter.p10db import migrate_p10

        settings = Settings.from_env()
        assert settings.scylla.keyspace.endswith("_it_p10"), "P10 tests only run in a *_it_p10 keyspace"
        _DB = Db.connect(settings.scylla)
        migrate(_DB)
        migrate_p10(_DB)
    return _DB


@pytest.fixture
def db():
    need_stack()
    from tests.v2.conftest import wipe

    d = p10_db()
    wipe(d)
    return d


@pytest.fixture
def qdrant() -> Iterator[TrackingIndex]:
    need_stack()
    settings = Settings.from_env()
    index = TrackingIndex(settings.index.url, settings.index.api_key, timeout_s=30)
    yield index
    for name in index.created:
        index.delete_collection(name)
