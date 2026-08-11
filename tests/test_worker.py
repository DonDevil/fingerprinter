import threading
import time

from work_queue.keys import CONSUMER_GROUP, state_key, stream_key
from work_queue.producer import JobProducer
from work_queue.state import JobStatus
from worker.fingerprint_worker import Worker

BLOCK_MS = 200


def test_consumer_group_can_read_a_job(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)

    entry = worker.claim_one()

    assert entry is not None
    assert entry.is_valid


def test_worker_receives_the_correct_job(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)

    entry = worker.claim_one()

    assert entry.job == sample_job


def test_job_state_becomes_claimed(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)

    worker.claim_one()

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.CLAIMED
    assert state["worker_id"] == "w1"
    assert state["attempt"] == "1"


def test_successful_processing_acks_the_job(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)
    entry = worker.claim_one()

    worker.ack(entry)

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED


def test_acknowledged_job_is_no_longer_pending(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)
    entry = worker.claim_one()

    worker.ack(entry)

    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0


def test_malformed_job_is_rejected_clearly(redis_client):
    redis_client.xadd(
        stream_key(),
        {
            "job_id": "bad-job",
            "media_evidence_id": "evidence-1",
            # media_url intentionally omitted
            "media_type": "video",
            "source_domain": "example.com",
            "target_id": "target-1",
            "target_version": "abc123",
            "techniques": "dinov2",
            "max_attempts": "3",
        },
    )
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)

    entry = worker.claim_one()

    assert entry is not None
    assert not entry.is_valid
    assert "media_url" in entry.error

    state = redis_client.hgetall(state_key("bad-job"))
    assert state["status"] == JobStatus.REJECTED

    # Rejected entries are ACKed immediately — they can never become valid
    # through redelivery, so they must not clog the pending list.
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0


def test_multiple_workers_do_not_receive_the_same_job_simultaneously(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS)

    entry_a = worker_a.claim_one()
    entry_b = worker_b.claim_one()

    assert entry_a is not None and entry_a.is_valid
    assert entry_b is None


def test_graceful_worker_shutdown(redis_client):
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS)
    handled = []

    thread = threading.Thread(target=worker.run, args=(handled.append,))
    thread.start()
    time.sleep(0.05)
    worker.stop()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert handled == []
