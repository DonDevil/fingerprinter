"""Operator CLI for the target lifecycle -- add / list / get / update
metadata / delete / build a target, plus the one-time `reindex` migration
(target-management design doc, S18/S21;
`docs/architecture/target-eager-build-audit.md`, Part B, for `build`).

    python -m target.cli add MEDIA_PATH --id ID --version VERSION [--metadata KEY=VALUE ...] [--json]
    python -m target.cli list [--json]
    python -m target.cli get ID --version VERSION [--json]
    python -m target.cli update-metadata ID --version VERSION [--set KEY=VALUE ...] [--unset KEY ...] [--json]
    python -m target.cli delete ID --version VERSION [--json]
    python -m target.cli build ID --version VERSION [--json]
    python -m target.cli reindex [--dry-run] [--json]

This module is a thin `TargetService` client: it parses arguments,
constructs a `TargetRegistry`/`TargetService` pair from the environment,
calls exactly one `TargetService` method, and formats the result or the
typed exception it raised. It does not validate target_id/target_version
itself, does not compute SHA-256, does not touch Redis or cache files
directly, and does not implement any lifecycle/deletion/reference logic --
all of that lives in `TargetService`/`TargetRegistry`. `build` is the one
exception to "exactly one `TargetService` method": it has no embedding
knowledge (`TargetService` is deliberately Redis/torch-free), so it calls
`target.build.build_target()` instead, reusing this module's own
registry/media_store wiring directly -- see `_cmd_build`.

Deliberately does NOT import `worker.main`, even though that module already
has the Redis/cache wiring this CLI needs (`WorkerConfig.from_env`,
`build_redis_client`, `build_registry`). `worker/main.py` imports
`embedding.dinov2_engine.DINOv2EmbeddingEngine` at module scope, pulling in
torch/transformers/numpy/Pillow -- a needless cost for every subcommand
except `build`. This module has its own small, self-contained env-driven
wiring instead, using the same environment variable names as the worker
(`REDIS_URL`, `TARGET_CACHE_PATH`, `SHARED_ARTIFACT_STORE_PATH`,
`EMBEDDING_DEVICE`, `TORCH_NUM_THREADS`) for operational consistency, and
the same `Redis.from_url(...)` + `ping()` fail-fast construction pattern.
`build`'s handler imports `DINOv2EmbeddingEngine` lazily, inside itself,
so every other subcommand stays torch-free
(`tests/test_embedding_lazy_import.py` proves this in a subprocess).

IMPORTANT: because cache cleanup on `delete`, media publication on
`create`, and the segment cache `build` writes into all operate on
whatever cache/media paths this process is wired to, this CLI must be run
with the same REDIS_URL / TARGET_CACHE_PATH / SHARED_ARTIFACT_STORE_PATH
configuration as the worker fleet it manages targets for -- otherwise
`delete` cleans up, or `build` populates, a cache directory no worker is
actually reading from.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from redis import Redis

from embedding.errors import EmbeddingError
from target.build import build_target
from target.cache import FilesystemEmbeddingCache
from target.errors import TargetServiceError
from target.identity import TargetRecord
from target.registry import ReindexResult, TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.service import TargetService
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedArtifactStoreError, SharedTargetMediaStore

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
DEFAULT_TARGET_CACHE_PATH = "./target_cache"

REDIS_SOCKET_CONNECT_TIMEOUT_S = 5
REDIS_SOCKET_TIMEOUT_S = 10

_EPILOG = (
    "IMPORTANT: run this CLI with the same REDIS_URL / TARGET_CACHE_PATH / "
    "SHARED_ARTIFACT_STORE_PATH configuration as the worker fleet it manages "
    "targets for -- delete/create operate on whatever cache and media paths "
    "this process is wired to."
)


# ---------------------------------------------------------------------------
# Environment wiring -- deliberately self-contained, see module docstring.
# ---------------------------------------------------------------------------


def _build_redis_client() -> Redis:
    redis_url = os.environ.get("REDIS_URL") or DEFAULT_REDIS_URL
    client = Redis.from_url(
        redis_url,
        decode_responses=True,
        socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT_S,
        socket_timeout=REDIS_SOCKET_TIMEOUT_S,
    )
    client.ping()
    return client


def _build_shared_store() -> Optional[SharedArtifactStore]:
    shared_path = os.environ.get("SHARED_ARTIFACT_STORE_PATH") or None
    return SharedArtifactStore(Path(shared_path)) if shared_path else None


def _build_media_store(store: Optional[SharedArtifactStore]) -> Optional[SharedTargetMediaStore]:
    return SharedTargetMediaStore(store) if store is not None else None


def _build_registry(
    redis_client: Redis, store: Optional[SharedArtifactStore], media_store: Optional[SharedTargetMediaStore]
) -> TargetRegistry:
    if store is not None:
        pooled_cache = SharedFilesystemEmbeddingCache(store, prefix="pooled")
        segment_cache = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
        return TargetRegistry(redis_client, pooled_cache, segment_cache, media_store=media_store)

    base = Path(os.environ.get("TARGET_CACHE_PATH") or DEFAULT_TARGET_CACHE_PATH)
    pooled_cache = FilesystemEmbeddingCache(base / "pooled")
    segment_cache = FilesystemSegmentEmbeddingCache(base / "segments")
    return TargetRegistry(redis_client, pooled_cache, segment_cache)


@dataclass
class _Context:
    """Everything a subcommand handler might need, built once per CLI
    invocation. Every subcommand uses `service`; only `build` additionally
    needs `registry`/`media_store` directly, since `TargetService` is
    deliberately Redis/torch-free and has no embedding-build method of its
    own (see module docstring)."""

    service: TargetService
    registry: TargetRegistry
    media_store: Optional[SharedTargetMediaStore]


def _build_context() -> _Context:
    redis_client = _build_redis_client()
    store = _build_shared_store()
    media_store = _build_media_store(store)
    registry = _build_registry(redis_client, store, media_store)
    return _Context(service=TargetService(registry), registry=registry, media_store=media_store)


def _getenv_optional_int(name: str) -> Optional[int]:
    raw = os.environ.get(name)
    if raw is None or raw == "":
        return None
    try:
        return int(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer, got {raw!r}") from exc


# ---------------------------------------------------------------------------
# Output formatting
# ---------------------------------------------------------------------------


def _record_to_dict(record: TargetRecord) -> dict:
    return {
        "target_id": record.target_id,
        "target_version": record.target_version,
        "media_path": record.media_path,
        "content_sha256": record.content_sha256,
        "media_metadata": dict(record.media_metadata),
        "created_at": record.created_at,
        "updated_at": record.updated_at,
    }


def _print_record_human(record: TargetRecord, extra: Optional[dict] = None) -> None:
    for key, value in (extra or {}).items():
        print(f"{key}: {value}")
    print(f"target_id: {record.target_id}")
    print(f"target_version: {record.target_version}")
    print(f"media_path: {record.media_path}")
    print(f"content_sha256: {record.content_sha256}")
    print(f"media_metadata: {json.dumps(dict(record.media_metadata), sort_keys=True)}")
    print(f"created_at: {record.created_at}")
    print(f"updated_at: {record.updated_at}")


def _print_json(payload) -> None:
    print(json.dumps(payload, sort_keys=True))


def _print_error(args: argparse.Namespace, error_type: str, message: str) -> None:
    if getattr(args, "json", False):
        _print_json({"error": error_type, "message": message})
    else:
        print(f"{error_type}: {message}", file=sys.stderr)


# ---------------------------------------------------------------------------
# Metadata KEY=VALUE parsing -- raw strings only, no implicit type coercion
# (design doc, S18): a convenience for flat operator tags, not a JSON editor.
# ---------------------------------------------------------------------------


def _parse_key_value(raw: str) -> tuple:
    if "=" not in raw:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE, got {raw!r}")
    key, _, value = raw.partition("=")
    if not key:
        raise argparse.ArgumentTypeError(f"expected KEY=VALUE with a non-empty KEY, got {raw!r}")
    return key, value


def _pairs_to_dict(pairs) -> dict:
    result: dict = {}
    for key, value in pairs or ():
        result[key] = value
    return result


# ---------------------------------------------------------------------------
# Subcommands
# ---------------------------------------------------------------------------


def _cmd_add(context: _Context, args: argparse.Namespace) -> None:
    metadata = _pairs_to_dict(args.metadata)
    record = context.service.create_target(args.id, args.version, args.media_path, metadata=metadata)
    if args.json:
        _print_json({"status": "ok", **_record_to_dict(record)})
    else:
        _print_record_human(record)


def _cmd_list(context: _Context, args: argparse.Namespace) -> None:
    records = context.service.list_targets()
    if args.json:
        _print_json([_record_to_dict(r) for r in records])
    else:
        for record in records:
            print(
                f"{record.target_id}\t{record.target_version}\t"
                f"{record.content_sha256[:12]}\t{record.updated_at}"
            )


def _cmd_get(context: _Context, args: argparse.Namespace) -> int:
    record = context.service.get_target(args.id, args.version)
    if record is None:
        if args.json:
            _print_json(None)
        else:
            print("not found", file=sys.stderr)
        return 1
    if args.json:
        _print_json(_record_to_dict(record))
    else:
        _print_record_human(record)
    return 0


def _cmd_update_metadata(context: _Context, args: argparse.Namespace) -> None:
    set_fields = _pairs_to_dict(args.set) if args.set else None
    record = context.service.update_target_metadata(
        args.id, args.version, set_fields=set_fields, remove_fields=args.unset or None
    )
    if args.json:
        _print_json(_record_to_dict(record))
    else:
        print(f"media_metadata: {json.dumps(dict(record.media_metadata), sort_keys=True)}")
        print(f"updated_at: {record.updated_at}")


def _cmd_delete(context: _Context, args: argparse.Namespace) -> None:
    context.service.delete_target(args.id, args.version)
    if args.json:
        _print_json({"status": "deleted", "target_id": args.id, "target_version": args.version})
    else:
        print(f"deleted {args.id}/{args.version}")


def _cmd_build(context: _Context, args: argparse.Namespace) -> int:
    """Eagerly build a target's segment embeddings (target-eager-build
    audit, Part B) -- the same `TargetRegistry.get_or_build_segment_embedding`
    call a live fingerprint job makes lazily, run here ahead of time so a
    slow/timeout-prone first build (e.g. a full-length movie) is discovered
    at operator-controlled time instead of inside a job.

    Not a `TargetService` method (see module docstring): constructs the
    embedding engine directly, lazily importing `DINOv2EmbeddingEngine`
    inside this function so every other subcommand stays torch-free."""
    record = context.service.get_target(args.id, args.version)
    if record is None:
        _print_error(args, "TargetNotFoundError", f"unknown target: {args.id!r} version {args.version!r}")
        return 1

    if not args.json:
        print(f"building {args.id}/{args.version} ...")

    try:
        torch_num_threads = _getenv_optional_int("TORCH_NUM_THREADS")
    except ValueError as exc:
        _print_error(args, "ConfigError", str(exc))
        return 1

    try:
        from embedding.dinov2_engine import DINOv2EmbeddingEngine

        engine = DINOv2EmbeddingEngine(
            device=os.environ.get("EMBEDDING_DEVICE") or "auto",
            torch_num_threads=torch_num_threads,
        )
        result = build_target(context.registry, engine, args.id, args.version, media_store=context.media_store)
    except (EmbeddingError, SharedArtifactStoreError, TimeoutError, ValueError) as exc:
        _print_error(args, type(exc).__name__, str(exc))
        return 1

    segment_count = len(result.entry.segments)
    total_duration_s = result.entry.segments[-1].end_time if result.entry.segments else 0.0
    status = "already_built" if result.already_built else "built"

    if args.json:
        _print_json(
            {
                "status": status,
                "target_id": result.target_id,
                "target_version": result.target_version,
                "segment_count": segment_count,
                "total_duration_s": total_duration_s,
            }
        )
    else:
        verb = "already built" if result.already_built else "built"
        print(f"{verb}: {result.target_id}/{result.target_version} ({segment_count} segments, {total_duration_s:.1f}s)")
    return 0


def _cmd_reindex(context: _Context, args: argparse.Namespace) -> None:
    result: ReindexResult = context.service.reindex(dry_run=args.dry_run)
    if args.json:
        _print_json(
            {
                "dry_run": args.dry_run,
                "found": [{"target_id": i, "target_version": v} for i, v in result.found],
                "added": [{"target_id": i, "target_version": v} for i, v in result.added],
            }
        )
    else:
        verb = "would add" if args.dry_run else "added"
        print(f"found {len(result.found)} target record(s); {verb} {len(result.added)} to the list index")
        for target_id, target_version in result.added:
            print(f"  {target_id}\t{target_version}")


# ---------------------------------------------------------------------------
# argparse wiring
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m target.cli", description=__doc__.splitlines()[0], epilog=_EPILOG)
    subparsers = parser.add_subparsers(dest="command", required=True)

    add_parser = subparsers.add_parser("add", help="Create a target")
    add_parser.add_argument("media_path")
    add_parser.add_argument("--id", required=True, dest="id")
    add_parser.add_argument("--version", required=True)
    add_parser.add_argument("--metadata", action="append", type=_parse_key_value, metavar="KEY=VALUE")
    add_parser.add_argument("--json", action="store_true")
    add_parser.set_defaults(func=_cmd_add)

    list_parser = subparsers.add_parser("list", help="List every registered target")
    list_parser.add_argument("--json", action="store_true")
    list_parser.set_defaults(func=_cmd_list)

    get_parser = subparsers.add_parser("get", help="Get one target")
    get_parser.add_argument("id")
    get_parser.add_argument("--version", required=True)
    get_parser.add_argument("--json", action="store_true")
    get_parser.set_defaults(func=_cmd_get)

    update_parser = subparsers.add_parser("update-metadata", help="Patch a target's metadata")
    update_parser.add_argument("id")
    update_parser.add_argument("--version", required=True)
    update_parser.add_argument("--set", action="append", type=_parse_key_value, metavar="KEY=VALUE")
    update_parser.add_argument("--unset", action="append", metavar="KEY")
    update_parser.add_argument("--json", action="store_true")
    update_parser.set_defaults(func=_cmd_update_metadata)

    delete_parser = subparsers.add_parser("delete", help="Delete a target")
    delete_parser.add_argument("id")
    delete_parser.add_argument("--version", required=True)
    delete_parser.add_argument("--json", action="store_true")
    delete_parser.set_defaults(func=_cmd_delete)

    build_parser = subparsers.add_parser("build", help="Eagerly build a target's segment embeddings")
    build_parser.add_argument("id")
    build_parser.add_argument("--version", required=True)
    build_parser.add_argument("--json", action="store_true")
    build_parser.set_defaults(func=_cmd_build)

    reindex_parser = subparsers.add_parser("reindex", help="One-time: backfill the target list index")
    reindex_parser.add_argument("--dry-run", action="store_true")
    reindex_parser.add_argument("--json", action="store_true")
    reindex_parser.set_defaults(func=_cmd_reindex)

    return parser


def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        context = _build_context()
        return args.func(context, args) or 0
    except TargetServiceError as exc:
        _print_error(args, type(exc).__name__, str(exc))
        return 1


if __name__ == "__main__":
    sys.exit(main())
