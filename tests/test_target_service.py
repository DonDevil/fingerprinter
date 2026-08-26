"""Target-management design doc — TargetService-level coverage: operator
validation (target_id/target_version charset+length, media_path filesystem
checks), the create-vs-conflict policy, and thin pass-through of
list/get/update/delete to TargetRegistry with typed-error translation.
"""
import os
import stat

import pytest

from target.cache import FilesystemEmbeddingCache
from target.errors import TargetAlreadyExistsError, TargetMediaError, TargetNotFoundError, TargetValidationError
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from target.service import TargetService


def _write(tmp_path, name, content: bytes):
    path = tmp_path / name
    path.write_bytes(content)
    return path


@pytest.fixture
def service(redis_client, tmp_path):
    pooled = FilesystemEmbeddingCache(tmp_path / "embedding-cache")
    segments = FilesystemSegmentEmbeddingCache(tmp_path / "segment-cache")
    registry = TargetRegistry(redis_client, pooled, segments)
    return TargetService(registry)


# ---------------------------------------------------------------------------
# CREATE — identifier validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bad_id",
    [
        "",
        "has:colon",
        "has space",
        " leading-space",
        "trailing-space ",
        "control\x01char",
        "a" * 129,
    ],
)
def test_create_target_rejects_invalid_target_id(service, tmp_path, bad_id):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    with pytest.raises(TargetValidationError):
        service.create_target(bad_id, "v1", str(media))


@pytest.mark.parametrize("bad_version", ["", "v:1", "v 1", "a" * 129])
def test_create_target_rejects_invalid_target_version(service, tmp_path, bad_version):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    with pytest.raises(TargetValidationError):
        service.create_target("blast", bad_version, str(media))


@pytest.mark.parametrize("good_id", ["blast", "avatar", "inception", "tamil_blasters", "movie-2026", "a", "A.1_2-3"])
def test_create_target_accepts_realistic_ids(service, tmp_path, good_id):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    record = service.create_target(good_id, "v1", str(media))
    assert record.target_id == good_id


def test_create_target_accepts_max_length_id(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    record = service.create_target("a" * 128, "v1", str(media))
    assert len(record.target_id) == 128


# ---------------------------------------------------------------------------
# CREATE — media validation
# ---------------------------------------------------------------------------


def test_create_target_rejects_missing_file(service, tmp_path):
    with pytest.raises(TargetMediaError):
        service.create_target("blast", "v1", str(tmp_path / "does-not-exist.mp4"))


def test_create_target_rejects_directory(service, tmp_path):
    directory = tmp_path / "a-directory"
    directory.mkdir()
    with pytest.raises(TargetMediaError):
        service.create_target("blast", "v1", str(directory))


def test_create_target_rejects_empty_file(service, tmp_path):
    media = _write(tmp_path, "empty.mp4", b"")
    with pytest.raises(TargetMediaError):
        service.create_target("blast", "v1", str(media))


def test_create_target_rejects_unreadable_file(service, tmp_path):
    if os.geteuid() == 0:
        pytest.skip("root ignores file permission bits")
    media = _write(tmp_path, "movie.mp4", b"bytes")
    media.chmod(0)
    try:
        with pytest.raises(TargetMediaError):
            service.create_target("blast", "v1", str(media))
    finally:
        media.chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_create_target_media_error_does_not_leak_raw_oserror(service, tmp_path):
    with pytest.raises(TargetMediaError) as exc_info:
        service.create_target("blast", "v1", str(tmp_path / "nope.mp4"))
    # A raw FileNotFoundError would also be caught by `except OSError`, so
    # assert the *exact* type surfaced is the typed wrapper, not the
    # built-in exception class create_target is supposed to translate.
    assert type(exc_info.value) is TargetMediaError


# ---------------------------------------------------------------------------
# CREATE — identity/conflict semantics
# ---------------------------------------------------------------------------


def test_create_target_valid(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"hello world")
    record = service.create_target("blast", "v1", str(media), metadata={"genre": "action"})

    assert record.target_id == "blast"
    assert record.target_version == "v1"
    assert record.media_metadata == {"genre": "action"}


def test_create_target_multiple_targets_independently_gettable(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    service.create_target("avatar", "v1", str(_write(tmp_path, "b.mp4", b"b")))

    assert service.get_target("blast", "v1") is not None
    assert service.get_target("avatar", "v1") is not None


def test_create_target_identical_content_retry_is_idempotent(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"identical bytes")
    first = service.create_target("blast", "v1", str(media))
    second = service.create_target("blast", "v1", str(media))

    assert first.created_at == second.created_at
    assert first.content_sha256 == second.content_sha256


def test_create_target_different_content_same_identity_raises_and_leaves_existing_unchanged(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"original bytes")
    original = service.create_target("blast", "v1", str(media))

    media.write_bytes(b"different bytes now")
    with pytest.raises(TargetAlreadyExistsError):
        service.create_target("blast", "v1", str(media))

    unchanged = service.get_target("blast", "v1")
    assert unchanged.content_sha256 == original.content_sha256


def test_create_target_same_content_under_new_version_succeeds(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"shared bytes")
    v1 = service.create_target("blast", "v1", str(media))
    v2 = service.create_target("blast", "v2", str(media))

    assert v1.content_sha256 == v2.content_sha256
    assert {r.target_version for r in service.list_targets()} == {"v1", "v2"}


def test_create_target_metadata_must_be_dict(service, tmp_path):
    media = _write(tmp_path, "movie.mp4", b"bytes")
    with pytest.raises(TargetValidationError):
        service.create_target("blast", "v1", str(media), metadata="not-a-dict")


# ---------------------------------------------------------------------------
# LIST / GET
# ---------------------------------------------------------------------------


def test_list_targets_passthrough(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    service.create_target("blast", "v2", str(_write(tmp_path, "b.mp4", b"b")))

    pairs = [(r.target_id, r.target_version) for r in service.list_targets()]
    assert pairs == [("blast", "v1"), ("blast", "v2")]


def test_get_target_existing(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    record = service.get_target("blast", "v1")
    assert record is not None
    assert record.target_id == "blast"


def test_get_target_missing_returns_none_not_exception(service):
    assert service.get_target("nope", "v1") is None


# ---------------------------------------------------------------------------
# UPDATE
# ---------------------------------------------------------------------------


def test_update_target_metadata_merge_and_remove(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")), metadata={"genre": "action"})

    updated = service.update_target_metadata("blast", "v1", set_fields={"region": "IN"}, remove_fields=["genre"])

    assert updated.media_metadata == {"region": "IN"}


def test_update_target_metadata_missing_target_raises(service):
    with pytest.raises(TargetNotFoundError):
        service.update_target_metadata("nope", "v1", set_fields={"a": "b"})


def test_update_target_metadata_set_fields_must_be_dict(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    with pytest.raises(TargetValidationError):
        service.update_target_metadata("blast", "v1", set_fields="not-a-dict")


def test_update_target_metadata_remove_fields_must_be_strings(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    with pytest.raises(TargetValidationError):
        service.update_target_metadata("blast", "v1", remove_fields=[123])


# ---------------------------------------------------------------------------
# DELETE
# ---------------------------------------------------------------------------


def test_delete_target_success(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    service.delete_target("blast", "v1")
    assert service.get_target("blast", "v1") is None


def test_delete_target_missing_raises_not_found(service):
    with pytest.raises(TargetNotFoundError):
        service.delete_target("nope", "v1")


# ---------------------------------------------------------------------------
# REINDEX passthrough
# ---------------------------------------------------------------------------


def test_reindex_passthrough(service, tmp_path):
    service.create_target("blast", "v1", str(_write(tmp_path, "a.mp4", b"a")))
    result = service.reindex(dry_run=True)
    assert ("blast", "v1") in result.found
