"""MinIO access (design §4.2) through the MinIO SDK (P9-D12).

Buckets: representations (``rep/<spec_hex>/<content_id>/<attempt_id>.npz``,
attempt-unique keys, never overwritten) and targets (``ref/<sha256_hex>``,
content-addressed after verified promotion; ``staging/<upload_id>``).
No bucket lifecycle rule is ever set: objects are deleted only by the orphan
sweeper, retirement or target purge.
"""

from __future__ import annotations

import hashlib
import io
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import IO, Any

import urllib3
from minio import Minio
from minio.commonconfig import REPLACE, ComposeSource, CopySource
from minio.error import S3Error

from fingerprinter.config import MinioConfig
from fingerprinter.coord import utc
from fingerprinter.errors import FingerprinterError, StorageUnavailableError

S3_SINGLE_COPY_LIMIT = 5 * 1024**3
"""Largest object a single server-side CopyObject may copy (S3 limit)."""
COPY_PART_BYTES = 512 * 1024**2
META_SHA = "sha256"


class IntegrityError(FingerprinterError):
    """Stored bytes do not have the expected size or SHA-256."""


class ObjectMissingError(FingerprinterError):
    pass


@dataclass(frozen=True)
class ObjInfo:
    bucket: str
    key: str
    size: int
    last_modified: datetime | None
    metadata: dict[str, str]


class _HashingReader(io.RawIOBase):
    def __init__(self, handle: IO[bytes]) -> None:
        self._h = handle
        self.sha = hashlib.sha256()
        self.count = 0

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        data = self._h.read(size)
        self.sha.update(data)
        self.count += len(data)
        return data


def _unavailable(exc: Exception) -> bool:
    return not isinstance(exc, S3Error)


class Objects:
    def __init__(self, client: Minio, cfg: MinioConfig) -> None:
        self.client = client
        self.cfg = cfg
        self.single_copy_limit = S3_SINGLE_COPY_LIMIT
        self.copy_part_bytes = COPY_PART_BYTES

    @classmethod
    def connect(cls, cfg: MinioConfig) -> Objects:
        http = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=10, read=120),
            retries=urllib3.Retry(total=3, backoff_factor=0.5, status_forcelist=[500, 502, 503]),
            maxsize=16,
        )
        client = Minio(
            cfg.endpoint,
            access_key=cfg.access_key,
            secret_key=cfg.secret_key,
            secure=cfg.secure,
            http_client=http,
        )
        return cls(client, cfg)

    def _call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        try:
            return fn(*args, **kwargs)
        except S3Error:
            raise
        except Exception as exc:  # noqa: BLE001 - urllib3/socket errors: storage unavailable
            raise StorageUnavailableError(f"minio: {type(exc).__name__}: {exc}") from exc

    def ensure_buckets(self) -> None:
        for bucket in (self.cfg.representations_bucket, self.cfg.targets_bucket):
            if not self._call(self.client.bucket_exists, bucket):
                try:
                    self._call(self.client.make_bucket, bucket)
                except S3Error as exc:
                    if exc.code not in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
                        raise

    # --- reads --------------------------------------------------------------

    def stat(self, bucket: str, key: str) -> ObjInfo | None:
        try:
            st = self._call(self.client.stat_object, bucket, key)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject", "ResourceNotFound"):
                return None
            raise StorageUnavailableError(f"minio stat: {exc.code}") from exc
        meta = {k.lower().removeprefix("x-amz-meta-"): v for k, v in (st.metadata or {}).items()}
        return ObjInfo(bucket, key, int(st.size or 0), utc(st.last_modified), meta)

    def stream(self, bucket: str, key: str, chunk: int = 1 << 20) -> Iterator[bytes]:
        try:
            resp = self._call(self.client.get_object, bucket, key)
        except S3Error as exc:
            if exc.code in ("NoSuchKey", "NoSuchObject"):
                raise ObjectMissingError(f"{bucket}/{key}") from exc
            raise StorageUnavailableError(f"minio get: {exc.code}") from exc
        try:
            while True:
                try:
                    data = resp.read(chunk)
                except Exception as exc:  # noqa: BLE001
                    raise StorageUnavailableError(f"minio read: {exc}") from exc
                if not data:
                    return
                yield data
        finally:
            resp.close()
            resp.release_conn()

    def read_verified(self, bucket: str, key: str, size: int, sha256_hex: str) -> bytes:
        data = b"".join(self.stream(bucket, key))
        if len(data) != size or hashlib.sha256(data).hexdigest() != sha256_hex:
            raise IntegrityError(f"{bucket}/{key}: size/sha256 mismatch on read")
        return data

    def list(self, bucket: str, prefix: str) -> Iterator[ObjInfo]:
        try:
            for obj in self.client.list_objects(bucket, prefix=prefix, recursive=True):
                yield ObjInfo(bucket, obj.object_name, int(obj.size or 0), utc(obj.last_modified), {})
        except S3Error as exc:
            raise StorageUnavailableError(f"minio list: {exc.code}") from exc
        except Exception as exc:  # noqa: BLE001
            raise StorageUnavailableError(f"minio list: {exc}") from exc

    def incomplete_uploads(self, bucket: str, prefix: str) -> list[tuple[str, str, datetime | None]]:
        """(key, upload_id, initiated) of unfinished multipart uploads (SDK-private API)."""
        result = self._call(self.client._list_multipart_uploads, bucket, prefix=prefix)
        return [(u.object_name, u.upload_id, utc(u.initiated_time)) for u in result.uploads]

    def abort_upload(self, bucket: str, key: str, upload_id: str) -> None:
        self._call(self.client._abort_multipart_upload, bucket, key, upload_id)

    # --- writes -------------------------------------------------------------

    def put_bytes(self, bucket: str, key: str, data: bytes, sha256_hex: str) -> None:
        """Upload and verify by read-back (artifacts are small: MiBs)."""
        self._call(
            self.client.put_object,
            bucket,
            key,
            io.BytesIO(data),
            len(data),
            metadata={META_SHA: sha256_hex},
        )
        self.read_verified(bucket, key, len(data), sha256_hex)

    def put_file_hashing(self, bucket: str, key: str, path: Path, size: int) -> str:
        """Multipart upload of ``path`` while hashing the bytes sent; returns their SHA-256."""
        with path.open("rb") as handle:
            reader = _HashingReader(handle)
            self._call(
                self.client.put_object, bucket, key, reader, size, part_size=64 * 1024**2
            )
        if reader.count != size:
            raise IntegrityError(f"sent {reader.count} bytes, expected {size}")
        return reader.sha.hexdigest()

    def copy_with_sha(self, bucket: str, src: str, dst: str, size: int, sha256_hex: str) -> None:
        """Server-side copy ``src`` → ``dst`` with metadata ``sha256``; multipart copy
        (compose of ranges) above the single-copy limit (design §10 step 5)."""
        meta = {META_SHA: sha256_hex}
        if size <= self.single_copy_limit:
            self._call(
                self.client.copy_object,
                bucket,
                dst,
                CopySource(bucket, src),
                metadata=meta,
                metadata_directive=REPLACE,
            )
            return
        sources = []
        offset = 0
        while offset < size:
            length = min(self.copy_part_bytes, size - offset)
            sources.append(ComposeSource(bucket, src, offset=offset, length=length))
            offset += length
        self._call(self.client.compose_object, bucket, dst, sources, metadata=meta)

    def delete(self, bucket: str, key: str) -> None:
        try:
            self._call(self.client.remove_object, bucket, key)
        except S3Error as exc:
            if exc.code not in ("NoSuchKey", "NoSuchObject"):
                raise StorageUnavailableError(f"minio delete: {exc.code}") from exc


def rep_key(spec_hex: str, content_id: str, attempt_id: str) -> str:
    return f"rep/{spec_hex}/{content_id}/{attempt_id}.npz"


def parse_rep_key(key: str) -> tuple[str, str, str] | None:
    parts = key.split("/")
    if len(parts) != 4 or parts[0] != "rep" or not parts[3].endswith(".npz"):
        return None
    return parts[1], parts[2], parts[3][: -len(".npz")]


def ref_key(sha256_hex: str) -> str:
    return f"ref/{sha256_hex}"


def uri(bucket: str, key: str) -> str:
    return f"s3://{bucket}/{key}"


def parse_uri(value: str) -> tuple[str, str]:
    rest = value.removeprefix("s3://")
    bucket, _, key = rest.partition("/")
    return bucket, key
