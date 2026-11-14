"""Spec lifecycle and retirement (design §3.7, §11.1, §11.4, P9-D3/D15).

``add`` (FP16 documents are always ``gated``) → ``enable`` (gated → active, only
with evidence that the precision gate and the labelled-decision gate passed) →
``set-default`` (refused for anything not ``active``) → ``deprecate`` →
``retire`` (refused while default, used by a non-terminal backfill, listed in
``retire_guard``, or while it is the only READY binding of an active target).
Retirement marks representation rows RETIRED; their objects are deleted only
after ``retire_grace_s``. Identities are never merged: enabling an FP16 spec
does not relabel any FP32 representation.
"""

from __future__ import annotations

import json
from datetime import timedelta
from pathlib import Path
from typing import Any

from fingerprinter.coord import utc
from fingerprinter.errors import FingerprinterError
from fingerprinter.objects import parse_uri
from fingerprinter.spec import RUNTIME_PROFILE, Spec
from fingerprinter.store import Store

REQUIRED_GATES = ("precision_gate", "labelled_decision_gate")


class SpecError(FingerprinterError):
    pass


class SpecLifecycle:
    def __init__(self, store: Store, objects: Any) -> None:
        self.store = store
        self.db = store.db
        self.objects = objects

    def add(self, spec: Spec, *, actor: str, origin_profile: str = RUNTIME_PROFILE) -> str:
        if self.store.profile(spec.document.runtime_profile) is None:
            raise SpecError(f"runtime profile {spec.document.runtime_profile} does not exist")
        status = "gated" if spec.compute_dtype == "float16" else "active"
        if not self.store.add_spec(spec, status=status, actor=actor, origin_profile=origin_profile):
            raise SpecError(f"spec {spec.digest} already exists")
        self.store.approve_compat(str(spec.digest), origin_profile, actor=actor, evidence="origin profile")
        return status

    def enable(self, digest: str, *, evidence: Path, actor: str) -> None:
        row = self.store.spec_row(digest)
        if row is None or row["status"] != "gated":
            raise SpecError("only a gated spec can be enabled")
        try:
            doc = json.loads(evidence.read_text())
        except (OSError, ValueError) as exc:
            raise SpecError(f"evidence unreadable: {exc}") from exc
        failed = [g for g in REQUIRED_GATES if str(doc.get(g, "")).upper() != "PASS"]
        if failed or doc.get("spec_digest") != digest:
            raise SpecError(f"evidence does not show PASS for {', '.join(failed) or 'this spec'}")
        if not self.store.set_spec_status(digest, expect="gated", status="active", actor=actor,
                                          evidence=str(evidence)):
            raise SpecError("spec changed concurrently")

    def set_default(self, digest: str, *, actor: str) -> None:
        if not self.store.set_role(digest, actor=actor):
            row = self.store.spec_row(digest)
            raise SpecError(f"cannot make {digest} default (status {row['status'] if row else 'missing'})")

    def deprecate(self, digest: str, *, actor: str) -> None:
        if not self.store.set_spec_status(digest, expect="active", status="deprecated", actor=actor):
            raise SpecError("only an active spec can be deprecated")

    def retire_blockers(self, digest: str) -> list[str]:
        out = []
        role = self.store.role("default")
        if role and role["spec_digest"] == digest:
            out.append("spec is the default")
        for job in self.db.rows("SELECT * FROM {ks}.backfill_jobs"):
            if job["state"] not in ("DONE", "CANCELLED") and digest in (job["from_spec"], job["to_spec"]):
                out.append(f"backfill {job['job_id']} uses it")
        for g in self.db.rows("SELECT * FROM {ks}.retire_guard WHERE spec_digest = ?", digest):
            out.append(f"retire_guard {g['holder']}")
        for t in self.db.rows("SELECT * FROM {ks}.targets"):
            if t["state"] != "READY":
                continue
            ready = [
                b for b in self.db.rows(
                    "SELECT * FROM {ks}.target_representations WHERE target_id = ? AND version = ?",
                    t["target_id"], t["version"],
                )
                if b["state"] == "READY"
            ]
            if ready and all(b["spec_digest"] == digest for b in ready):
                out.append(f"only READY binding of active target {t['target_id']} v{t['version']}")
        return out

    def retire(self, digest: str, *, actor: str) -> int:
        blockers = self.retire_blockers(digest)
        if blockers:
            raise SpecError("retire refused: " + "; ".join(blockers))
        row = self.store.spec_row(digest)
        if row is None or row["status"] not in ("deprecated", "active"):
            raise SpecError("only an active or deprecated spec can be retired")
        if not self.store.set_spec_status(digest, expect=row["status"], status="retired", actor=actor):
            raise SpecError("spec changed concurrently")
        now = self.store.now()
        count = 0
        for bucket in range(self.store.settings.backfill.buckets):
            for r in self.db.rows(
                "SELECT content_id FROM {ks}.representations_by_spec WHERE spec_digest = ? AND bucket = ?",
                digest, bucket,
            ):
                self.db.lwt(
                    "UPDATE {ks}.representations SET state = 'RETIRED', retired_at = ? "
                    "WHERE content_id = ? AND spec_digest = ? IF state = 'READY'",
                    now, r["content_id"], digest,
                )
                count += 1
        return count

    def retire_sweep(self) -> int:
        """Delete objects of RETIRED representations older than ``retire_grace_s``."""
        cutoff = self.store.now() - timedelta(seconds=self.store.settings.retire_grace_s)
        deleted = 0
        for spec_row in self.store.specs():
            if spec_row["status"] != "retired":
                continue
            digest = spec_row["spec_digest"]
            for bucket in range(self.store.settings.backfill.buckets):
                for r in self.db.rows(
                    "SELECT content_id FROM {ks}.representations_by_spec WHERE spec_digest = ? "
                    "AND bucket = ?", digest, bucket,
                ):
                    rep = self.store.representation(r["content_id"], digest)
                    if rep is None or rep["state"] != "RETIRED" or utc(rep["retired_at"]) > cutoff:
                        continue
                    b, k = parse_uri(rep["artifact_uri"])
                    if self.objects.stat(b, k) is not None:
                        self.objects.delete(b, k)
                        deleted += 1
        return deleted
