"""Job schema for the Redis fingerprint job contract.

A stream entry *is* the job spec (immutable, per the architecture proposal).
This module only knows how to fold a Job to/from the flat string->string
mapping that XADD/XREADGROUP deal in.

Phase 12 adds `schema_version`: every other durable contract in this project
(`work_queue.results.RESULT_SCHEMA_VERSION`,
`target.cache.CACHE_ENTRY_SCHEMA_VERSION`,
`target.segment_cache.SEGMENT_CACHE_ENTRY_SCHEMA_VERSION`) already carries an
explicit schema version; the job contract was the one exception, and it is
exactly the contract with the most producers (per
`docs/design/design-proposal-1.md` §2, "N crawler machines: fire-and-forget
XADD producers" — independently deployed, independently upgraded). The field
is additive and backward compatible: `to_stream_fields()` always writes it,
but `from_stream_fields()` treats a missing value as `1` (an entry written by
a producer that predates this field is unambiguously schema 1, the only
schema that has ever existed) rather than rejecting it. A *present but
mismatched* value is rejected — a worker has no logic to interpret a job
shape it doesn't know, so guessing would be worse than refusing (mirrors
`target/cache.py`/`target/segment_cache.py`'s exact-match-only
`*_SCHEMA_VERSION` check, not a range check).

`created_at` is deliberately not a field here: a Redis Stream entry ID is
already `<ms-since-epoch>-<seq>` (assigned by `XADD ... *`), so job creation
time is already recoverable from the entry ID without duplicating it into
the payload — see `integration/timing.py`.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping

JOB_SCHEMA_VERSION = 1

REQUIRED_FIELDS = (
    "job_id",
    "media_evidence_id",
    "media_url",
    "media_type",
    "source_domain",
    "target_id",
    "target_version",
    "techniques",
    "max_attempts",
)


class JobValidationError(ValueError):
    """A stream entry does not satisfy the job contract."""


@dataclass(frozen=True)
class Job:
    job_id: str
    media_evidence_id: str
    media_url: str
    media_type: str
    source_domain: str
    target_id: str
    target_version: str
    techniques: tuple[str, ...]
    max_attempts: int
    schema_version: int = field(default=JOB_SCHEMA_VERSION)

    def to_stream_fields(self) -> dict[str, str]:
        """Flatten to the string->string mapping XADD requires."""
        return {
            "job_id": self.job_id,
            "media_evidence_id": self.media_evidence_id,
            "media_url": self.media_url,
            "media_type": self.media_type,
            "source_domain": self.source_domain,
            "target_id": self.target_id,
            "target_version": self.target_version,
            "techniques": ",".join(self.techniques),
            "max_attempts": str(self.max_attempts),
            "schema_version": str(self.schema_version),
        }

    @staticmethod
    def from_stream_fields(fields: Mapping[str, str]) -> "Job":
        missing = [name for name in REQUIRED_FIELDS if not fields.get(name)]
        if missing:
            raise JobValidationError(f"missing required field(s): {', '.join(missing)}")

        raw_schema_version = fields.get("schema_version")
        if raw_schema_version:
            try:
                schema_version = int(raw_schema_version)
            except ValueError as exc:
                raise JobValidationError("schema_version must be an integer") from exc
            if schema_version != JOB_SCHEMA_VERSION:
                raise JobValidationError(
                    f"unsupported job schema_version: {schema_version} "
                    f"(this worker supports {JOB_SCHEMA_VERSION})"
                )
        else:
            # No producer has ever written a schema before version 1 existed,
            # so an absent field is schema 1, not "unknown" — see module docstring.
            schema_version = JOB_SCHEMA_VERSION

        try:
            max_attempts = int(fields["max_attempts"])
        except ValueError as exc:
            raise JobValidationError("max_attempts must be an integer") from exc
        if max_attempts < 1:
            raise JobValidationError("max_attempts must be >= 1")

        techniques = tuple(t for t in fields["techniques"].split(",") if t)
        if not techniques:
            raise JobValidationError("techniques must contain at least one entry")

        return Job(
            job_id=fields["job_id"],
            media_evidence_id=fields["media_evidence_id"],
            media_url=fields["media_url"],
            media_type=fields["media_type"],
            source_domain=fields["source_domain"],
            target_id=fields["target_id"],
            target_version=fields["target_version"],
            techniques=techniques,
            max_attempts=max_attempts,
            schema_version=schema_version,
        )
