"""Phase 5: worker acquisition integration.

Proves the intended chain: claim -> acquire -> handler receives a
MediaArtifact -> synthetic Result -> Phase 4 result commit. Also proves
acquisition failures map onto Phase 3's TransientFailure/PermanentFailure
rather than a new retry mechanism.
"""
from acquisition import MediaAcquirer
from work_queue.keys import retry_zset_key, state_key
from work_queue.producer import JobProducer
from work_queue.results import ResultStore
from work_queue.state import JobStatus
from worker.acquisition_handler import build_acquisition_handler
from worker.fingerprint_worker import Worker

BLOCK_MS = 200
LEASE_MS = 500


def _worker(redis_client, **overrides):
    kwargs = dict(block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    kwargs.update(overrides)
    return Worker(redis_client, **kwargs)


def _handler():
    return build_acquisition_handler(MediaAcquirer(connect_timeout_s=2.0, read_timeout_s=2.0))


def test_worker_acquires_media_and_commits_synthetic_result(redis_client, make_job, media_server):
    job = make_job(media_url=media_server.url("/ok"))
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _handler())

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.COMPLETED

    record = ResultStore(redis_client).get(job.job_id)
    assert record is not None
    assert record["algorithm"] == "acquisition-only-v0"


def test_permanent_acquisition_failure_maps_to_permanent_worker_failure(
    redis_client, make_job, media_server
):
    job = make_job(media_url=media_server.url("/notfound"), max_attempts=3)
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client)

    entry = worker.claim_one()
    worker.process_claim(entry, _handler())

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.FAILED  # terminal immediately, max_attempts irrelevant
    assert redis_client.zcard(retry_zset_key()) == 0  # no retry was ever scheduled


def test_transient_acquisition_failure_schedules_retry(redis_client, make_job, media_server):
    job = make_job(media_url=media_server.url("/error"), max_attempts=3)
    JobProducer(redis_client).enqueue(job)
    worker = _worker(redis_client, retry_base_delay_s=0.05, retry_max_delay_s=0.2)

    entry = worker.claim_one()
    worker.process_claim(entry, _handler())

    state = redis_client.hgetall(state_key(job.job_id))
    assert state["status"] == JobStatus.RETRY_SCHEDULED
    assert redis_client.zcard(retry_zset_key()) == 1
