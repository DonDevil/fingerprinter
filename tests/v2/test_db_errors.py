"""Scylla driver errors are typed (no raw driver exception escapes ``Db``).

Found by SOAK-1000 run 1: a ``WriteFailure`` (code 1500, a replica failed to
execute the Paxos write under load) escaped ``Db.lwt`` raw and killed an
encoder worker thread.
"""

from __future__ import annotations

import pytest
from cassandra import ConsistencyLevel, ReadFailure, WriteFailure, WriteTimeout, WriteType

from fingerprinter.db import Db, LwtUnknownError
from fingerprinter.errors import StorageUnavailableError


class _Stmt:
    consistency_level = None
    serial_consistency_level = None


class _Session:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def prepare(self, cql: str) -> _Stmt:
        return _Stmt()

    def execute(self, stmt, values):
        raise self.exc


def _db(exc: Exception) -> Db:
    return Db(cluster=None, session=_Session(exc), keyspace="ks")


def _write_failure() -> WriteFailure:
    return WriteFailure("Operation failed", consistency=ConsistencyLevel.LOCAL_SERIAL,
                        required_responses=1, received_responses=0, failures=1,
                        write_type=WriteType.CAS)


@pytest.mark.parametrize("exc", [_write_failure(), WriteTimeout("timed out", write_type=WriteType.CAS)])
def test_lwt_write_failure_or_timeout_is_unknown_outcome(exc):
    with pytest.raises(LwtUnknownError):  # a StorageUnavailableError: callers resolve by SERIAL read
        _db(exc).lwt("UPDATE {ks}.t SET a = 1 WHERE k = 1 IF EXISTS")


@pytest.mark.parametrize("exc", [_write_failure(), ReadFailure("Operation failed", failures=1)])
def test_plain_statement_failures_are_storage_unavailable(exc):
    with pytest.raises(StorageUnavailableError):
        _db(exc).rows("SELECT * FROM {ks}.t")
