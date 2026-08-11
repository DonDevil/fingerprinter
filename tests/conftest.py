"""Shared fixtures for the Redis job contract tests.

Tests run against a dedicated Redis database (index 15 by default) so they
never touch a real job stream. The db is flushed before and after each test.
"""
import os

import pytest
from redis import Redis

from tests.media_test_server import MediaTestServer
from work_queue.jobs import Job

TEST_REDIS_URL = os.environ.get("FINGERPRINTER_TEST_REDIS_URL", "redis://localhost:6379/15")


@pytest.fixture
def redis_client():
    client = Redis.from_url(TEST_REDIS_URL, decode_responses=True)
    client.flushdb()
    yield client
    client.flushdb()
    client.close()


@pytest.fixture
def make_job():
    def _make_job(**overrides) -> Job:
        defaults = dict(
            job_id="job-1",
            media_evidence_id="evidence-1",
            media_url="https://example.com/video.mp4",
            media_type="video",
            source_domain="example.com",
            target_id="target-1",
            target_version="abc123",
            techniques=("dinov2",),
            max_attempts=3,
        )
        defaults.update(overrides)
        return Job(**defaults)

    return _make_job


@pytest.fixture
def sample_job(make_job) -> Job:
    return make_job()


@pytest.fixture(scope="module")
def media_server():
    server = MediaTestServer()
    yield server
    server.shutdown()
