"""TargetService — the operator-facing target lifecycle boundary.

Application-level layer above `TargetRegistry` (target-management design
doc, S4/S5). `TargetService` owns operator-facing validation (target_id/
target_version charset+length, media_path filesystem checks, the
create-vs-conflict policy) and translates `TargetRegistry`'s lower-level
behavior into the small, typed error set in `target/errors.py`.

`TargetService` never touches Redis, never constructs a Redis key or Set
member, never computes a cache filename, and never knows
`SharedArtifactStore` paths -- every one of those responsibilities stays in
`TargetRegistry` and its injected collaborators (design doc, S4.1's
responsibility table). This is what lets a future HTTP layer sit directly
on top of `TargetService` without learning anything about the storage
underneath it (design doc, S19).

The CLI (`target/cli.py`) is the first, but not the only intended, client
of this class.
"""
from __future__ import annotations

import re
from pathlib import Path
from typing import Optional, Sequence, Union

from target.errors import (
    TargetAlreadyExistsError,
    TargetMediaError,
    TargetNotFoundError,
    TargetServiceError,
    TargetValidationError,
    TargetLockTimeoutError,
)
from target.identity import TargetRecord
from target.registry import ReindexResult, TargetRegistry

__all__ = [
    "TargetService",
    "TargetServiceError",
    "TargetValidationError",
    "TargetMediaError",
    "TargetNotFoundError",
    "TargetAlreadyExistsError",
    "TargetLockTimeoutError",
    "ReindexResult",
]

# Operator-boundary identifier contract (design doc, S6): letters, digits,
# '.', '_', '-' only. Excludes ':' (closes target_key()'s unescaped-':'
# collision class) and, being a strict allow-list, automatically excludes
# whitespace and control characters without a separate check.
_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9._-]+$")
_IDENTIFIER_MAX_LENGTH = 128


def _validate_identifier(value: str, field_name: str) -> None:
    if (
        not isinstance(value, str)
        or not (1 <= len(value) <= _IDENTIFIER_MAX_LENGTH)
        or not _IDENTIFIER_PATTERN.match(value)
    ):
        raise TargetValidationError(
            f"invalid {field_name}: {value!r} -- must be 1-{_IDENTIFIER_MAX_LENGTH} characters "
            f"from [A-Za-z0-9._-], no whitespace, ':', or control characters"
        )


def _validate_media_path(media_path: Union[str, Path]) -> Path:
    path = Path(media_path)

    if not path.exists():
        raise TargetMediaError(f"media_path does not exist: {path}")
    if path.is_dir():
        raise TargetMediaError(f"media_path is a directory, not a file: {path}")
    if not path.is_file():
        raise TargetMediaError(f"media_path is not a regular file: {path}")

    try:
        size = path.stat().st_size
    except OSError as exc:
        raise TargetMediaError(f"media_path could not be inspected: {path} ({exc})") from exc
    if size == 0:
        raise TargetMediaError(f"media_path is empty: {path}")

    try:
        with open(path, "rb") as f:
            f.read(1)
    except OSError as exc:
        raise TargetMediaError(f"media_path is not readable: {path} ({exc})") from exc

    return path


def _validate_metadata(metadata: Optional[dict], field_name: str = "metadata") -> dict:
    if metadata is None:
        return {}
    if not isinstance(metadata, dict):
        raise TargetValidationError(f"{field_name} must be a dict or None, got {type(metadata).__name__}")
    return metadata


def _validate_remove_fields(remove_fields: Optional[Sequence[str]]) -> Sequence[str]:
    if remove_fields is None:
        return ()
    if not all(isinstance(key, str) for key in remove_fields):
        raise TargetValidationError("remove_fields must contain only strings")
    return remove_fields


class TargetService:
    """The application-level target lifecycle boundary. Thin: every method
    validates its own inputs and then delegates to exactly one
    `TargetRegistry` call for the actual mutation/read."""

    def __init__(self, registry: TargetRegistry):
        self._registry = registry

    def create_target(
        self,
        target_id: str,
        target_version: str,
        media_path: Union[str, Path],
        metadata: Optional[dict] = None,
    ) -> TargetRecord:
        """Create `(target_id, target_version)`, or accept a no-op retry if
        the content is byte-identical to what's already registered.

        Raises `TargetAlreadyExistsError` if the identity already exists
        with *different* content -- create_target never silently swaps
        content behind an existing version (design doc, S7). To register
        different content, call this again with a new `target_version`; to
        change only metadata, use `update_target_metadata`."""
        _validate_identifier(target_id, "target_id")
        _validate_identifier(target_version, "target_version")
        path = _validate_media_path(media_path)
        metadata = _validate_metadata(metadata)

        return self._registry.register_target(
            target_id,
            target_version,
            str(path),
            media_metadata=metadata,
            on_conflict="reject",
        )

    def list_targets(self) -> list[TargetRecord]:
        """Every registered target, deterministically ordered. O(number of
        registered targets) -- see `TargetRegistry.list_targets`."""
        return self._registry.list_targets()

    def get_target(self, target_id: str, target_version: str) -> Optional[TargetRecord]:
        """`None` on a miss -- a plain lookup, not a lifecycle error."""
        return self._registry.get_target(target_id, target_version)

    def update_target_metadata(
        self,
        target_id: str,
        target_version: str,
        set_fields: Optional[dict] = None,
        remove_fields: Optional[Sequence[str]] = None,
    ) -> TargetRecord:
        """Shallow-merge `set_fields` into the existing metadata, then
        remove every key named in `remove_fields` (a key in both ends up
        removed). Never touches media_path/content_sha256/target_id/
        target_version. Raises `TargetNotFoundError` if the identity
        doesn't exist."""
        set_fields = _validate_metadata(set_fields, field_name="set_fields")
        remove_fields = _validate_remove_fields(remove_fields)

        return self._registry.update_target_metadata(
            target_id, target_version, set_fields=set_fields, remove_fields=remove_fields
        )

    def delete_target(self, target_id: str, target_version: str) -> None:
        """Delete the target and its exclusive artifacts; retain shared
        media still referenced by another target; leave historical results
        and queued/in-flight jobs untouched (design doc, S12/S13). Raises
        `TargetNotFoundError` if the identity doesn't exist."""
        self._registry.delete_target(target_id, target_version)

    def reindex(self, dry_run: bool = False) -> ReindexResult:
        """One-time, explicit migration/repair: backfill
        `fingerprint:target:index` from target records that predate it
        (design doc, S21). Purely additive; never touches existing target
        records, caches, jobs, or results. Not run automatically anywhere
        -- only ever invoked explicitly, e.g. via `target.cli reindex`."""
        return self._registry.reindex(dry_run=dry_run)
