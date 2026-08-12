"""Acquisition-specific failure classification.

Two roots only, matching Phase 3's worker-level distinction
(`TransientFailure` / `PermanentFailure`) so a caller can map one-to-one
without inventing a second retry taxonomy:

- `TransientAcquisitionError` — the same URL might succeed on a later
  attempt (network blip, rate limit, server hiccup).
- `PermanentAcquisitionError` — retrying changes nothing (bad scheme, the
  resource doesn't exist, the content isn't usable media).

This module has no dependency on `worker`/`work_queue` — it is usable by
any caller, not just the queue worker. The worker-side mapping onto
`TransientFailure`/`PermanentFailure` lives in
`worker/acquisition_handler.py`.
"""
from __future__ import annotations


class AcquisitionError(Exception):
    """Base class for every media-acquisition failure."""


class TransientAcquisitionError(AcquisitionError):
    """Retryable: the same URL may succeed on a later attempt."""


class PermanentAcquisitionError(AcquisitionError):
    """Not retryable: retrying this URL cannot change the outcome."""


class UnsupportedSchemeError(PermanentAcquisitionError):
    """URL scheme is not http/https (or whatever the acquirer allows)."""


class ConnectionTimeoutError(TransientAcquisitionError):
    """Failed to establish a connection within connect_timeout_s."""


class ReadTimeoutError(TransientAcquisitionError):
    """Connected, but no data arrived within read_timeout_s."""


class NetworkError(TransientAcquisitionError):
    """Other transient network failure (DNS, reset, interrupted transfer)."""


class RedirectLimitExceededError(PermanentAcquisitionError):
    """The redirect chain exceeded max_redirects."""


class UnsafeDestinationError(PermanentAcquisitionError):
    """Resolved destination IP is loopback/private/link-local/reserved/etc.

    Permanent, not transient: the URL's resolved address is inherently
    unsafe, so retrying the same candidate cannot change the outcome (and
    must not cause the worker to hammer the same address indefinitely).
    """


class _HTTPStatusMixin:
    """Carries the offending status code alongside the message."""

    def __init__(self, status_code: int, message: str = ""):
        self.status_code = status_code
        super().__init__(message or f"HTTP {status_code}")


class NotFoundError(_HTTPStatusMixin, PermanentAcquisitionError):
    """HTTP 404 — the resource does not exist."""


class GoneError(_HTTPStatusMixin, PermanentAcquisitionError):
    """HTTP 410 — the resource is permanently gone."""


class ClientError(_HTTPStatusMixin, PermanentAcquisitionError):
    """Other 4xx — malformed/unauthorized request, not retryable as-is."""


class RateLimitedError(_HTTPStatusMixin, TransientAcquisitionError):
    """HTTP 429 — retryable after backoff."""


class ServerError(_HTTPStatusMixin, TransientAcquisitionError):
    """HTTP 5xx — retryable, the origin may recover."""


class UnexpectedStatusError(_HTTPStatusMixin, PermanentAcquisitionError):
    """Any status code outside the ranges classified above."""


class UnsupportedContentTypeError(PermanentAcquisitionError):
    """Content-Type is not in the acquirer's allowed set."""


class SizeLimitExceededError(PermanentAcquisitionError):
    """The download exceeded max_bytes while streaming."""


class InvalidMediaError(PermanentAcquisitionError):
    """The downloaded bytes failed basic media validation (corrupt/empty)."""
