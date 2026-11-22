"""Shared fixtures for the Redis job contract tests.

Tests run against a dedicated Redis database (index 15, default port 16380 = a throwaway container, never 6379) so they
never touch a real job stream. The db is flushed before and after each test.
"""
import os
from urllib.parse import urlparse

import pytest
from redis import Redis

from tests.media_test_server import MediaTestServer
from work_queue.jobs import Job

TEST_REDIS_URL = os.environ.get("FINGERPRINTER_TEST_REDIS_URL", "redis://127.0.0.1:16380/15")


def _assert_disposable_redis(url: str) -> None:
    """Never FLUSHDB a shared Redis (WP1 Run 3 incident: system Redis :6379 db15 was flushed)."""
    port = urlparse(url).port
    if port in (6379, 16379):
        pytest.exit(f"refusing to run: FINGERPRINTER_TEST_REDIS_URL={url} is a shared/system Redis; "
                    "start a throwaway one (docker run --rm -p 127.0.0.1:16380:6379 redis:7.4.2-alpine)", 2)


@pytest.fixture
def redis_client():
    _assert_disposable_redis(TEST_REDIS_URL)
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
