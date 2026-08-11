"""Phase 2: Redis PEL lease / crash-recovery tests.

Uses a small test-only lease_ms so idle-timeout waits stay in the
low-hundreds-of-milliseconds range instead of arbitrary sleeps.
"""
import threading
import time

from work_queue.keys import CONSUMER_GROUP, state_key, stream_key
from work_queue.producer import JobProducer
from work_queue.state import JobStatus
from worker.fingerprint_worker import Worker

BLOCK_MS = 200
LEASE_MS = 150
LEASE_S = LEASE_MS / 1000


def _wait_past_lease(margin_s: float = 0.1) -> None:
    time.sleep(LEASE_S + margin_s)


def test_no_premature_reclaim_before_lease_expiry(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=LEASE_MS)

    worker_a.claim_one()
    reclaimed = worker_b.reclaim_stale()

    assert reclaimed == []


def test_crash_recovery_reclaims_and_reprocesses(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=LEASE_MS)

    entry_a = worker_a.claim_one()
    assert entry_a.is_valid
    assert entry_a.attempt == 1
    # worker_a "crashes" here: no ack, no further contact.

    _wait_past_lease()
    reclaimed = worker_b.reclaim_stale()

    assert len(reclaimed) == 1
    entry_b = reclaimed[0]
    assert entry_b.is_valid
    assert entry_b.job.job_id == sample_job.job_id
    assert entry_b.attempt == 2  # attempt count reflects the reprocessing

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.CLAIMED
    assert state["worker_id"] == "worker-b"
    assert state["attempt"] == "2"

    completed = worker_b.ack(entry_b)
    assert completed is True

    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED

    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0

    # worker_a wakes up post-crash and tries to finalize its stale claim.
    stale_completed = worker_a.ack(entry_a)
    assert stale_completed is False

    # worker_b's completion must survive worker_a's stale finalize attempt.
    state = redis_client.hgetall(state_key(sample_job.job_id))
    assert state["status"] == JobStatus.COMPLETED
    assert state["attempt"] == "2"
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0


def test_multiple_workers_do_not_repeatedly_steal_a_live_reclaimed_job(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_b = Worker(redis_client, consumer_name="worker-b", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_c = Worker(redis_client, consumer_name="worker-c", block_ms=BLOCK_MS, lease_ms=LEASE_MS)

    worker_a.claim_one()
    _wait_past_lease()

    reclaimed_b = worker_b.reclaim_stale()
    assert len(reclaimed_b) == 1

    # worker_c immediately tries to steal the same entry: worker_b's
    # XAUTOCLAIM just reset the idle timer, so it isn't stale yet.
    reclaimed_c = worker_c.reclaim_stale()
    assert reclaimed_c == []


def test_run_loop_reclaims_stale_jobs_through_the_normal_handler_path(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker_a = Worker(redis_client, consumer_name="worker-a", block_ms=BLOCK_MS, lease_ms=LEASE_MS)
    worker_a.claim_one()  # claims then "crashes" — never acked

    _wait_past_lease()

    worker_b = Worker(
        redis_client,
        consumer_name="worker-b",
        block_ms=BLOCK_MS,
        lease_ms=LEASE_MS,
        reclaim_interval_ms=LEASE_MS,
    )
    handled = []
    thread = threading.Thread(target=worker_b.run, args=(handled.append,))
    thread.start()
    time.sleep(0.08)  # let the first reclaim+handler+ack pass complete
    worker_b.stop()
    thread.join(timeout=2)

    assert not thread.is_alive()
    assert len(handled) == 1
    assert handled[0].job_id == sample_job.job_id

    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 0


def test_graceful_shutdown_does_not_ack_unfinished_work(redis_client, sample_job):
    JobProducer(redis_client).enqueue(sample_job)
    worker = Worker(redis_client, consumer_name="w1", block_ms=BLOCK_MS, lease_ms=LEASE_MS)

    entry = worker.claim_one()
    assert entry.is_valid
    worker.stop()

    # Shutdown must never ack a job the handler hasn't finished — it must
    # stay pending so another worker can recover it via XAUTOCLAIM.
    pending = redis_client.xpending(stream_key(), CONSUMER_GROUP)
    assert pending["pending"] == 1
