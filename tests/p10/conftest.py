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


# --- the P10 stack: P9 services on the test keyspace + a P10System on the dev Qdrant ---------------

VECTORS = Path.home() / "anti_piracy/datasets/p10/vectors"


class Hooks:
    """Fault-injection points (test build only; production passes a no-op)."""

    def __init__(self) -> None:
        self.handlers: dict[str, Any] = {}
        self.log: list[tuple[float, str, dict]] = []

    def __call__(self, point: str, **ctx: Any) -> None:
        import time as _t

        self.log.append((_t.monotonic(), point, ctx))
        h = self.handlers.get(point)
        if h is not None:
            h(**ctx)

    def on(self, point: str, fn: Any) -> None:
        self.handlers[point] = fn

    def clear(self) -> None:
        self.handlers.clear()


def p10_settings(tag: str, **matching: Any):
    from tests.v2.conftest import make_settings

    base = make_settings(tag)
    return base.model_copy(update={
        "index": base.index.model_copy(update={"collection_prefix": "t" + tag[:8], "active_ttl_s": 0.2,
                                               "mutation_lease_s": 1.0, "clock_margin_s": 0.0,
                                               "drain_grace_s": 12.0, "build_lease_s": 30.0}),
        "matching": base.matching.model_copy(update={"task_lease_s": 10.0, "backoff_base_s": 0.2, "backoff_max_s": 1.0,
                                                     "search_timeout_s": 2.0,
                                                     "poll_interval_s": 0.05, **matching}),
        "matcher_reconcile": base.matcher_reconcile.model_copy(update={"scan_rows_per_s": 100000.0,
                                                                       "intent_grace_s": 0.0,
                                                                       "repair_backoff_base_s": 0.2,
                                                                       "repair_backoff_max_s": 1.0}),
    })


class P10Stack:
    def __init__(self, stack: Any, sysm: Any, hooks: Hooks, index: Any, tmp: Path) -> None:
        self.s = stack            # the P9 Stack (tests/v2/conftest.py)
        self.sys = sysm
        self.hooks = hooks
        self.index = index
        self.tmp = tmp
        self.spec = str(stack.spec.digest)
        self._n = 0

    @staticmethod
    def artifact(vectors, pts_s):
        import numpy as np

        from fingerprinter import artifact as art
        from fingerprinter.commit import Artifact

        v = np.asarray(vectors, dtype=np.float32)
        idx = np.arange(len(v))
        seg = art.Segments(idx, np.round(np.asarray(pts_s) * 1000).astype(np.int64), (1, 1000),
                           np.asarray(pts_s, dtype=np.float64), v, 5.0)
        data, sha = art.build(seg, {"test": True})
        return Artifact(data, sha, len(v), float(pts_s[-1] + 5.0) if len(v) else 0.0, "{}", "cpu", "float32")

    def ingest_media(self, vectors, pts_s, data: bytes | None = None) -> str:
        """A media content through P9's request → claim → commit protocol (representation.ready XADDed)."""
        from fingerprinter.coord import uuid7
        from tests.v2.helpers import request_env

        self._n += 1
        env = request_env(data or f"media-{self._n}-{uuid.uuid4()}".encode(), f"https://m.example/{self._n}.mp4")
        self.s.requests.upsert_event(env)
        cid = str(env.payload.content.content_id)
        row = self.s.requests.claim(cid, "default", uuid7())
        assert self.s.committer.commit(row, self.s.spec, self.artifact(vectors, pts_s)) == "won"
        return cid

    def register_target(self, vectors, pts_s, title: str = "t") -> tuple[str, int, str]:
        """A target through P9's TargetManager; its reference representation committed by P9's committer,
        whose follow-up binds the target READY. Returns (target_id, version, content_id)."""
        from fingerprinter.coord import uuid7

        self._n += 1
        path = self.tmp / f"ref-{self._n}.bin"
        path.write_bytes(f"ref-{self._n}-{uuid.uuid4()}".encode())
        reg = self.s.targets.register(path, title=title, kind="movie", actor="test")
        req = next(r for r in self.s.requests.all() if r["content_id"] == reg.content_id)
        row = self.s.requests.claim(reg.content_id, req["spec_key"], uuid7())
        assert self.s.committer.commit(row, self.s.spec, self.artifact(vectors, pts_s)) == "won"
        return reg.target_id, reg.version, reg.content_id

    def bootstrap(self, roles=("target", "media")) -> None:
        """First build of each role from authoritative state, then promotion."""
        for role in roles:
            res = self.sys.rebuild(self.spec, role, promote=True)
            assert res.get("promoted"), res

    def media(self, cid: str):
        return self.sys.p.media_subject(cid, self.spec)

    def target(self, tid: str, ver: int):
        return self.sys.p.target_subject(tid, ver, self.spec)

    def run_matcher(self) -> dict:
        import threading

        return self.sys.matcher_loop(threading.Event(), until_idle=True)

    def reconcile(self, force: bool = True) -> dict:
        return self.sys.reconcile_once(force=force)

    def drive(self, rounds: int = 6) -> None:
        """Alternate reconciler passes and matcher rounds until quiet (hints, scans, repairs, reruns)."""
        for _ in range(rounds):
            self.reconcile()
            self.run_matcher()


@pytest.fixture
def p10(tmp_path):
    need_stack()
    from fingerprinter.commit import Committer
    from fingerprinter.confidence import UNAVAILABLE, params_from_tune_eval
    from fingerprinter.coord import Coord, ShiftedClock
    from fingerprinter.objects import Objects
    from fingerprinter.p10ctx import P10
    from fingerprinter.p10system import P10System
    from fingerprinter.publish import Publisher
    from fingerprinter.reconcile import Reconciler
    from fingerprinter.requests import Requests
    from fingerprinter.spec import dinov2_seg_v2
    from fingerprinter.store import Store
    from fingerprinter.targets import TargetManager
    from tests.v2.conftest import Stack, register_default_spec, wipe

    tag = uuid.uuid4().hex[:10]
    settings = p10_settings(tag)
    db = p10_db()
    wipe(db)
    coord = Coord.connect(settings.redis)
    clock = ShiftedClock(coord.clock)
    coord.clock = clock
    objects = Objects.connect(settings.minio)
    objects.ensure_buckets()
    store = Store(db, coord, settings)
    pub = Publisher(store)
    pub.seed()
    spec = dinov2_seg_v2()
    register_default_spec(store, spec)
    req = Requests(store, pub)
    com = Committer(store, req, objects)
    rec = Reconciler(store, req, com, objects)
    st = Stack(settings, db, coord, objects, store, clock, pub, req, com, rec, spec)
    st.targets = TargetManager(st).install()
    hooks = Hooks()
    index = TrackingIndex(settings.index.url, settings.index.api_key, timeout_s=30)
    sysm = P10System(P10(settings, db, coord, objects, store, index, hook=hooks))
    sysm.migrate()
    sysm.init_spec(str(spec.digest), actor="test")
    tune = Path(__file__).resolve().parents[2] / "benchmarks/p10-index/results/tune-eval.json"
    report = json.loads(tune.read_text())
    params, cal, status = params_from_tune_eval(report)
    assert status == UNAVAILABLE
    sysm.techniques.register("2.1", params, calibration=cal, status=status, calibration_id=None, actor="test",
                             evidence="benchmarks/p10-index/results/tune-eval.json")
    yield P10Stack(st, sysm, hooks, index, tmp_path)
    for name in index.created:
        index.delete_collection(name)
    for key in coord.client.scan_iter(match=f"fpit:{tag}:*"):
        coord.client.delete(key)
    for key in coord.client.scan_iter(match=f"fpit-events:{tag}:*"):
        coord.client.delete(key)
    for bucket in (settings.minio.representations_bucket, settings.minio.targets_bucket):
        for obj in objects.list(bucket, ""):
            objects.delete(bucket, obj.key)
        for key, upload_id, _ in objects.incomplete_uploads(bucket, ""):
            objects.abort_upload(bucket, key, upload_id)
        objects.client.remove_bucket(bucket)
    coord.client.close()
