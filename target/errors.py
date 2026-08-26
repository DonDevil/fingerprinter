"""Typed errors for the target lifecycle (create/list/get/update/delete).

Deliberately its own module rather than living in `target/service.py`.
`TargetRegistry.register_target(..., on_conflict="reject")` (used by
`TargetService.create_target`) needs to raise `TargetAlreadyExistsError`
itself, and `TargetRegistry.update_target_metadata`/`delete_target` need to
raise `TargetNotFoundError` themselves — so both `target/registry.py` (the
lower-level module) and `target/service.py` (the higher-level module built
on top of it) need these classes. Defining them in `target/service.py` and
importing them into `target/registry.py` would make the lower-level module
depend on the higher-level one — backwards, and unnecessary. A standalone,
dependency-neutral module both can import from avoids that direction
entirely, at zero behavioral cost: the class names, hierarchy, and
`except` behavior are exactly what the design specifies either way.

Follows this repository's existing convention of a small, flat
`SomethingError(BuiltinError)` per concern — see `JobValidationError`
(`work_queue/jobs.py`), `CandidateValidationError`
(`integration/candidate.py`), `ConfigError` (`worker/main.py`),
`SharedArtifactStoreError` (`target/shared_storage.py`) — never a deep
hierarchy.
"""
from __future__ import annotations


class TargetServiceError(Exception):
    """Base class for every typed error the target lifecycle (TargetService
    and TargetRegistry's lifecycle methods) raises. Rarely raised directly."""


class TargetValidationError(TargetServiceError, ValueError):
    """target_id/target_version fails the operator-boundary charset/length
    contract, or metadata/set_fields/remove_fields has the wrong shape."""


class TargetMediaError(TargetServiceError, OSError):
    """media_path is missing, is a directory, is empty, or is unreadable —
    never a raw OSError/FileNotFoundError/IsADirectoryError leaks past
    TargetService.create_target."""


class TargetNotFoundError(TargetServiceError, KeyError):
    """A mutation (update_target_metadata, delete_target) referenced a
    (target_id, target_version) that does not exist. `get_target` itself
    returns None on a miss, not this."""


class TargetAlreadyExistsError(TargetServiceError, ValueError):
    """create_target's (target_id, target_version) already exists with
    different content. Raised directly by
    TargetRegistry.register_target(..., on_conflict="reject")."""


class TargetLockTimeoutError(TargetServiceError, TimeoutError):
    """Could not acquire the target-record lifecycle lock within the poll
    budget — another create/update/delete for the same identity is in
    progress."""
