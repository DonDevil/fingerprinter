"""Redis key/name conventions for the fingerprint job contract."""

CONSUMER_GROUP = "fingerprinter-workers"
DEFAULT_PRIORITY = "default"


def stream_key(priority: str = DEFAULT_PRIORITY) -> str:
    return f"fingerprint:jobs:stream:{priority}"


def state_key(job_id: str) -> str:
    return f"fingerprint:job:{job_id}:state"


def retry_zset_key(priority: str = DEFAULT_PRIORITY) -> str:
    return f"fingerprint:retry:delayed:{priority}"
