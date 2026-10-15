"""P9 test fixtures.

Integration tests run against the **real** Scylla, Redis and MinIO of crawler2's
compose stack (``. scripts/p9-env.sh``; ``RUN_P9_INTEGRATION=1``). Isolation:
keyspace ``fingerprinter_it_p9`` (every partition deleted before each test),
per-test Redis key and stream prefixes, per-test MinIO buckets (emptied and
removed afterwards). Unit tests (no marker) need no services.
"""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from typing import Any

import pytest

from fingerprinter.commit import Committer
from fingerprinter.config import Settings
from fingerprinter.coord import Coord, ShiftedClock
from fingerprinter.db import Db, migrate
from fingerprinter.objects import Objects
from fingerprinter.publish import Publisher
from fingerprinter.reconcile import Reconciler
from fingerprinter.requests import Requests
from fingerprinter.runtime import RuntimeFingerprint, RuntimeProfile
from fingerprinter.spec import RUNTIME_PROFILE, Spec, dinov2_seg_v2
from fingerprinter.store import Store
from fingerprinter.targets import TargetManager

INTEGRATION = os.environ.get("RUN_P9_INTEGRATION") == "1"
integration = pytest.mark.skipif(not INTEGRATION, reason="needs the stack: . scripts/p9-env.sh")

_DB: Db | None = None
_FP: dict[str, RuntimeFingerprint] = {}


def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "integration: needs real Scylla/Redis/MinIO")
    config.addinivalue_line("markers", "slow: long-running (model inference, large objects)")


def fingerprint(device: str = "cpu") -> RuntimeFingerprint:
    if device not in _FP:
        _FP[device] = RuntimeFingerprint.capture(device)
    return _FP[device]


def _db(settings: Settings) -> Db:
    global _DB
    if _DB is None:
        _DB = Db.connect(settings.scylla)
        migrate(_DB)
    return _DB


def wipe(db: Db) -> None:
    """Delete every partition of every table of the test keyspace (tiny data)."""
    tables = db.rows(
        "SELECT table_name FROM system_schema.tables WHERE keyspace_name = ?", db.keyspace
    )
    for t in tables:
        name = t["table_name"]
        cols = db.rows(
            "SELECT column_name, kind, position FROM system_schema.columns "
            "WHERE keyspace_name = ? AND table_name = ?",
            db.keyspace,
            name,
        )
        pk = [c["column_name"] for c in sorted(cols, key=lambda c: c["position"]) if c["kind"] == "partition_key"]
        sel = ", ".join(pk)
        where = " AND ".join(f"{c} = ?" for c in pk)
        for row in db.rows(f"SELECT DISTINCT {sel} FROM {{ks}}.{name}"):
            db.run(f"DELETE FROM {{ks}}.{name} WHERE {where}", *[row[c] for c in pk])


@dataclass
class Stack:
    settings: Settings
    db: Db
    coord: Coord
    objects: Objects
    store: Store
    clock: ShiftedClock
    pub: Publisher
    requests: Requests
    committer: Committer
    reconciler: Reconciler
    spec: Spec
    targets: Any = None

    def fresh(self, role: str = "encoder", **overrides: Any) -> Stack:
        """Another 'process' on the same durable state (separate store/instance)."""
        settings = self.settings.model_copy(update=overrides) if overrides else self.settings
        store = Store(self.db, self.coord, settings, role=role)
        store.instance = f"{settings.host_id}:{role}:{uuid.uuid4().hex[:8]}"
        store.clock = self.clock
        pub = Publisher(store)
        req = Requests(store, pub)
        com = Committer(store, req, self.objects)
        rec = Reconciler(store, req, com, self.objects)
        out = Stack(settings, self.db, self.coord, self.objects, store, self.clock, pub, req, com, rec, self.spec)
        out.targets = TargetManager(out).install()
        return out


def make_settings(tag: str, **overrides: Any) -> Settings:
    base = Settings.from_env()
    update = {
        "host_id": "it-host-1",
        "redis": base.redis.model_copy(
            update={"prefix": f"fpit:{tag}:", "stream_prefix": f"fpit-events:{tag}:"}
        ),
        "minio": base.minio.model_copy(
            update={
                "representations_bucket": f"fp-it-{tag}-reps",
                "targets_bucket": f"fp-it-{tag}-targets",
            }
        ),
        "intake": base.intake.model_copy(update={"reader": f"reader-{tag}"}),
    }
    update.update(overrides)
    return base.model_copy(update=update)


def register_default_spec(store: Store, spec: Spec, *, devices: tuple[str, ...] = ("cpu",)) -> None:
    prints = [fingerprint(d) for d in devices]
    profile = RuntimeProfile.from_fingerprints(RUNTIME_PROFILE, *prints)
    store.put_profile(profile, actor="test")
    store.add_spec(spec, status="active", actor="test", origin_profile=RUNTIME_PROFILE)
    store.approve_compat(str(spec.digest), RUNTIME_PROFILE, actor="test", evidence="test")
    store.set_role(str(spec.digest), actor="test")


@pytest.fixture
def stack() -> Iterator[Stack]:
    if not INTEGRATION:
        pytest.skip("needs the stack: . scripts/p9-env.sh")
    tag = uuid.uuid4().hex[:10]
    settings = make_settings(tag)
    db = _db(settings)
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
    yield st
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


# --- encoder fixtures -----------------------------------------------------------

_EMBEDDERS: dict[tuple[str, str], Any] = {}


def embedders(device: str = "cpu", dtypes: tuple[str, ...] = ("float32",)) -> dict[str, Any]:
    from fingerprinter.model import Embedder

    out = {}
    for dtype in dtypes:
        key = (device, dtype)
        if key not in _EMBEDDERS:
            _EMBEDDERS[key] = Embedder(device, dtype, threads=4)
        out[dtype] = _EMBEDDERS[key]
    return out


def ensure_group(stack: Stack, stream_name: str, group: str = "media-registry") -> None:
    """What crawler2's ``RedisStreamReader.ensure_group`` does (ID 0, MKSTREAM)."""
    import redis as _redis

    try:
        stack.coord.client.xgroup_create(stack.coord.stream(stream_name), group, id="0", mkstream=True)
    except _redis.ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def make_encoder(stack: Stack, tmp_path, *, device: str = "cpu", host_id: str | None = None,
                 dtypes: tuple[str, ...] = ("float32",), golden: bool = False, **encoder_cfg: Any):
    from fingerprinter.encoder import Encoder

    cfg = stack.settings.encoder.model_copy(update={"decode_threads": 2, "poll_interval_s": 0.1,
                                                   "cpu_batch": 8, **encoder_cfg})
    acq = stack.settings.acquire.model_copy(update={"ssrf_allow_cidrs": ["127.0.0.0/8"]})
    settings = stack.settings.model_copy(update={
        "encoder": cfg, "acquire": acq, "scratch_dir": tmp_path / f"scratch-{host_id or 'h1'}",
        "host_id": host_id or stack.settings.host_id,
        "roles": ["encoder", "scheduler"],
    })
    view = stack.fresh("encoder", **{k: getattr(settings, k) for k in
                                     ("encoder", "acquire", "scratch_dir", "host_id", "roles")})
    return Encoder(view, device=device, golden_check=golden, calibrate=device == "cuda",
                   embedders=embedders(device, dtypes), fingerprint=fingerprint(device)), view


@pytest.fixture
def web(tmp_path):
    from tests.v2.webserver import FixtureServer

    root = tmp_path / "web"
    root.mkdir()
    server = FixtureServer(root)
    yield server
    server.close()


def settings_env(settings: Settings, **extra: str) -> dict[str, str]:
    """Environment for a fingerprinter subprocess sharing this test's durable state."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("FINGERPRINTER_")}

    def put(prefix: str, model: Any) -> None:
        for name, value in model.model_dump().items():
            if isinstance(value, dict):
                continue
            if isinstance(value, list):
                value = ",".join(str(v) for v in value)
            if value is None:
                continue
            env[f"FINGERPRINTER_{prefix}{name.upper()}"] = str(value)

    put("", settings)
    for section in ("scylla", "redis", "minio", "encoder", "acquire", "intake", "publisher",
                    "reconciler", "backfill"):
        put(f"{section.upper()}__", getattr(settings, section))
    env.update(extra)
    env["PYTHONUNBUFFERED"] = "1"
    return env
