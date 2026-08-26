"""Operator CLI for the target lifecycle -- add / list / get / update
metadata / delete a target, plus the one-time `reindex` migration
(target-management design doc, S18/S21).

    python -m target.cli add MEDIA_PATH --id ID --version VERSION [--metadata KEY=VALUE ...] [--json]
    python -m target.cli list [--json]
    python -m target.cli get ID --version VERSION [--json]
    python -m target.cli update-metadata ID --version VERSION [--set KEY=VALUE ...] [--unset KEY ...] [--json]
    python -m target.cli delete ID --version VERSION [--json]
    python -m target.cli reindex [--dry-run] [--json]

This module is a thin `TargetService` client: it parses arguments,
constructs a `TargetRegistry`/`TargetService` pair from the environment,
calls exactly one `TargetService` method, and formats the result or the
typed exception it raised. It does not validate target_id/target_version
itself, does not compute SHA-256, does not touch Redis or cache files
directly, and does not implement any lifecycle/deletion/reference logic --
all of that lives in `TargetService`/`TargetRegistry`.

Deliberately does NOT import `worker.main`, even though that module already
has the Redis/cache wiring this CLI needs (`WorkerConfig.from_env`,
`build_redis_client`, `build_registry`). `worker/main.py` imports
`embedding.dinov2_engine.DINOv2EmbeddingEngine` at module scope, pulling in
torch/transformers/numpy/Pillow -- a needless cost for a command that only
reads/writes a few small Redis hashes. This module has its own small,
self-contained env-driven wiring instead, using the same environment
variable names as the worker (`REDIS_URL`, `TARGET_CACHE_PATH`,
`SHARED_ARTIFACT_STORE_PATH`) for operational consistency, and the same
`Redis.from_url(...)` + `ping()` fail-fast construction pattern.

IMPORTANT: because cache cleanup on `delete` and media publication on
`create` operate on whatever cache/media paths this process is wired to,
this CLI must be run with the same REDIS_URL / TARGET_CACHE_PATH /
SHARED_ARTIFACT_STORE_PATH configuration as the worker fleet it manages
targets for -- otherwise `delete` cleans up a cache directory no worker is
actually reading from.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

from redis import Redis

from target.cache import FilesystemEmbeddingCache
from target.errors import TargetServiceError
from target.identity import TargetRecord
from target.registry import ReindexResult, TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.service import TargetService
from target.shared_cache import SharedFilesystemEmbeddingCache, SharedFilesystemSegmentEmbeddingCache
from target.shared_storage import SharedArtifactStore, SharedTargetMediaStore

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


def _build_registry(redis_client: Redis) -> TargetRegistry:
    shared_path = os.environ.get("SHARED_ARTIFACT_STORE_PATH") or None
    if shared_path:
        store = SharedArtifactStore(Path(shared_path))
        media_store = SharedTargetMediaStore(store)
        pooled_cache = SharedFilesystemEmbeddingCache(store, prefix="pooled")
        segment_cache = SharedFilesystemSegmentEmbeddingCache(store, prefix="segments")
        return TargetRegistry(redis_client, pooled_cache, segment_cache, media_store=media_store)

    base = Path(os.environ.get("TARGET_CACHE_PATH") or DEFAULT_TARGET_CACHE_PATH)
    pooled_cache = FilesystemEmbeddingCache(base / "pooled")
    segment_cache = FilesystemSegmentEmbeddingCache(base / "segments")
    return TargetRegistry(redis_client, pooled_cache, segment_cache)


def _build_service() -> TargetService:
    redis_client = _build_redis_client()
    return TargetService(_build_registry(redis_client))


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


def _cmd_add(service: TargetService, args: argparse.Namespace) -> None:
    metadata = _pairs_to_dict(args.metadata)
    record = service.create_target(args.id, args.version, args.media_path, metadata=metadata)
    if args.json:
        _print_json({"status": "ok", **_record_to_dict(record)})
    else:
        _print_record_human(record)


def _cmd_list(service: TargetService, args: argparse.Namespace) -> None:
    records = service.list_targets()
    if args.json:
        _print_json([_record_to_dict(r) for r in records])
    else:
        for record in records:
            print(
                f"{record.target_id}\t{record.target_version}\t"
                f"{record.content_sha256[:12]}\t{record.updated_at}"
            )


def _cmd_get(service: TargetService, args: argparse.Namespace) -> int:
    record = service.get_target(args.id, args.version)
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


def _cmd_update_metadata(service: TargetService, args: argparse.Namespace) -> None:
    set_fields = _pairs_to_dict(args.set) if args.set else None
    record = service.update_target_metadata(
        args.id, args.version, set_fields=set_fields, remove_fields=args.unset or None
    )
    if args.json:
        _print_json(_record_to_dict(record))
    else:
        print(f"media_metadata: {json.dumps(dict(record.media_metadata), sort_keys=True)}")
        print(f"updated_at: {record.updated_at}")


def _cmd_delete(service: TargetService, args: argparse.Namespace) -> None:
    service.delete_target(args.id, args.version)
    if args.json:
        _print_json({"status": "deleted", "target_id": args.id, "target_version": args.version})
    else:
        print(f"deleted {args.id}/{args.version}")


def _cmd_reindex(service: TargetService, args: argparse.Namespace) -> None:
    result: ReindexResult = service.reindex(dry_run=args.dry_run)
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

    reindex_parser = subparsers.add_parser("reindex", help="One-time: backfill the target list index")
    reindex_parser.add_argument("--dry-run", action="store_true")
    reindex_parser.add_argument("--json", action="store_true")
    reindex_parser.set_defaults(func=_cmd_reindex)

    return parser


def main(argv: Optional[list] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    try:
        service = _build_service()
        return args.func(service, args) or 0
    except TargetServiceError as exc:
        if getattr(args, "json", False):
            _print_json({"error": type(exc).__name__, "message": str(exc)})
        else:
            print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
