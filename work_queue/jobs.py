"""Job schema for the Redis fingerprint job contract.

A stream entry *is* the job spec (immutable, per the architecture proposal).
This module only knows how to fold a Job to/from the flat string->string
mapping that XADD/XREADGROUP deal in.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping

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
        }

    @staticmethod
    def from_stream_fields(fields: Mapping[str, str]) -> "Job":
        missing = [name for name in REQUIRED_FIELDS if not fields.get(name)]
        if missing:
            raise JobValidationError(f"missing required field(s): {', '.join(missing)}")

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
        )
