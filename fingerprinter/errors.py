"""Typed errors (DoD-4: no silent degradation).

``EncodeError`` carries the contract's ``EncodeFailure`` class, whether a later
attempt could succeed, and a short detail. ``scope`` says what the failure is
attributed to: ``source`` (only this source is unusable), ``content`` (the bytes
themselves, so every source fails the same way) or ``attempt`` (this attempt).
"""

from __future__ import annotations

from antipiracy_contracts.events.fingerprinting import EncodeFailure


class FingerprinterError(Exception):
    """Base class."""


class StorageUnavailableError(FingerprinterError):
    """Scylla, Redis or MinIO did not answer; nothing was decided."""


class ConfigError(FingerprinterError):
    pass


class RuntimeIncompatibleError(FingerprinterError):
    """The encoder's runtime fingerprint is not allowed to produce a spec (§3.5)."""


class LeaseLostError(FingerprinterError):
    """A fenced transition failed: another owner superseded this one."""


class EncodeError(FingerprinterError):
    def __init__(
        self,
        failure: EncodeFailure,
        detail: str,
        *,
        retryable: bool,
        scope: str = "attempt",
    ) -> None:
        super().__init__(f"{failure.value}: {detail}")
        self.failure = failure
        self.detail = detail[:200]
        self.retryable = retryable
        self.scope = scope


class DeviceError(FingerprinterError):
    """A CUDA error other than OOM (class 4, design §12.4)."""


class SingleItemOOMError(FingerprinterError):
    """CUDA OOM at batch size 1 (class 2, design §12.4)."""
