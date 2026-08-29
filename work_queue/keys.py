"""Redis key/name conventions for the fingerprint job contract."""

CONSUMER_GROUP = "fingerprinter-workers"
DEFAULT_PRIORITY = "default"


def stream_key(priority: str = DEFAULT_PRIORITY) -> str:
    return f"fingerprint:jobs:stream:{priority}"


def state_key(job_id: str) -> str:
    return f"fingerprint:job:{job_id}:state"


def retry_zset_key(priority: str = DEFAULT_PRIORITY) -> str:
    return f"fingerprint:retry:delayed:{priority}"


def result_key(job_id: str) -> str:
    return f"fingerprint:job:{job_id}:result"


def results_stream_key(priority: str = DEFAULT_PRIORITY) -> str:
    return f"fingerprint:results:stream:{priority}"


def match_index_key(target_id: str, target_version: str) -> str:
    """ZSET of job_ids whose committed result was a MATCH against this
    target/version, member=job_id, score=processing_completed_at. Written
    atomically by `Worker.commit_result` alongside the result hash/state/
    event -- never a separate index-maintenance pass."""
    return f"fingerprint:matches:target:{target_id}:{target_version}"
