"""work_queue/admin.py -- full-namespace Redis reset used by
`target.cli clear-db` (see that module's tests for the CLI-level behavior).
"""
from work_queue.admin import clear_all


def test_clear_all_default_excludes_target_and_lock_keys(redis_client):
    redis_client.set("fingerprint:job:job-1:state", "x")
    redis_client.set("fingerprint:results:stream:default", "x")
    redis_client.set("fingerprint:target:t1:v1", "x")
    redis_client.set("fingerprint:lock:target-record:t1:v1", "x")
    redis_client.set("unrelated:key", "x")  # different namespace entirely

    deleted = clear_all(redis_client)

    assert deleted == 2
    assert redis_client.exists("fingerprint:job:job-1:state") == 0
    assert redis_client.exists("fingerprint:results:stream:default") == 0
    assert redis_client.exists("fingerprint:target:t1:v1") == 1
    assert redis_client.exists("fingerprint:lock:target-record:t1:v1") == 1
    assert redis_client.exists("unrelated:key") == 1


def test_clear_all_include_targets_wipes_target_and_lock_keys_too(redis_client):
    redis_client.set("fingerprint:target:t1:v1", "x")
    redis_client.set("fingerprint:lock:target-record:t1:v1", "x")

    deleted = clear_all(redis_client, include_targets=True)

    assert deleted == 2
    assert redis_client.exists("fingerprint:target:t1:v1") == 0
    assert redis_client.exists("fingerprint:lock:target-record:t1:v1") == 0


def test_clear_all_on_empty_namespace_deletes_nothing(redis_client):
    assert clear_all(redis_client) == 0
