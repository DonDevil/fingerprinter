"""Job-creation timestamp, recovered from the Redis Stream entry ID rather
than stored as a duplicate field on `work_queue.jobs.Job`.

`XADD stream * ...` assigns an ID of the form `<ms-since-epoch>-<seq>`
(Redis's own stream-entry-id format) at the moment the entry is appended —
see the Redis Streams documentation. `work_queue.producer.JobProducer
.enqueue()` already returns this ID; `integration.submission
.FingerprintJobSubmitter.submit()` threads it through
`SubmissionResult.entry_id` for exactly this purpose. Recovering
`created_at` from it avoids adding a field to `Job` that Redis already
records for free — see `work_queue/jobs.py`'s module docstring.
"""
from __future__ import annotations


def created_at_from_entry_id(entry_id: str) -> float:
    """Seconds since the epoch, parsed from a Redis Stream entry ID.

    Raises `ValueError` if `entry_id` isn't in the `<ms>-<seq>` shape —
    a caller should only ever pass an ID Redis itself generated.
    """
    ms_part = entry_id.split("-", 1)[0]
    return int(ms_part) / 1000.0
