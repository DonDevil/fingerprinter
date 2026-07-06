import os
import tempfile
import pytest
from work_queue.job_queue import JobQueue

@pytest.fixture
def temp_db():
    db_fd, db_path = tempfile.mkstemp()
    os.close(db_fd)
    yield db_path
    os.remove(db_path)

def test_add_and_claim_job(temp_db):
    queue = JobQueue(db_path=temp_db)
    queue.add_job("http://example.com/video.mp4", priority=5)
    assert queue.pending_count() == 1
    job = queue.claim_job()
    assert job is not None
    assert job["url"] == "http://example.com/video.mp4"
    queue.update_job_status(job["id"], "done")
    assert queue.pending_count() == 0
    queue.close()
