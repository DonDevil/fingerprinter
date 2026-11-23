"""FP-09: one correlation identifier traced through the V1 fingerprint path, and the documented V2 gap.

The correlation key of the V1 contract is ``media_evidence_id`` (the crawler's opaque reference, see
integration/candidate.py and README "correlated back to the caller's own media_evidence_id"). The V2/P10 event contract carries
``correlation_id`` / ``causation_id`` on the envelope (crawler2 contracts). Uses the throwaway Redis (:16380/15) and the loopback
media server only; no production event is published.

The trace is also written to ``$FP09_TRACE_OUT`` when set.
"""
from __future__ import annotations

import json
import os
import time
import uuid
from pathlib import Path

import pytest

from acquisition import MediaAcquirer
from embedding.config import SegmentSamplingConfig
from embedding.dinov2_engine import DINOv2EmbeddingEngine
from integration.candidate import FingerprintCandidate
from integration.outcome import FingerprintOutcome, resolve_outcome
from integration.submission import FingerprintJobSubmitter
from target.cache import FilesystemEmbeddingCache
from target.registry import TargetRegistry
from target.segment_cache import FilesystemSegmentEmbeddingCache
from work_queue.keys import result_key, results_stream_key, stream_key
from worker.fingerprint_worker import Worker
from worker.matching_handler import build_matching_handler

TINY = Path(__file__).parent / "fixtures" / "tiny_video.mp4"


@pytest.fixture(scope="module")
def engine():
    return DINOv2EmbeddingEngine(device="cpu", segment_sampling_config=SegmentSamplingConfig(segment_duration_s=0.5))


@pytest.fixture
def registry(redis_client, tmp_path):
    return TargetRegistry(redis_client, FilesystemEmbeddingCache(tmp_path / "e"), FilesystemSegmentEmbeddingCache(tmp_path / "s"))


def _entries(r, key):
    return [dict(f) for _, f in r.xrange(key)]


def test_v1_correlation_id_end_to_end_trace(redis_client, media_server, engine, registry):
    cid = f"fp09-{uuid.uuid4()}"
    trace: list[dict] = []

    def hop(stage: str, value) -> None:
        trace.append({"t": round(time.time(), 3), "stage": stage, "correlation_id": value})
        assert value == cid, f"correlation id changed at {stage}: {value!r}"

    registry.register_target("target-1", "v1", str(TINY))
    cand = FingerprintCandidate(candidate_url=media_server.url("/video"), media_evidence_id=cid, media_type="video",
                                source_domain="synthetic.invalid", target_id="target-1", target_version="v1")
    hop("task_creation (candidate)", cand.media_evidence_id)
    sub = FingerprintJobSubmitter(redis_client).submit(cand)
    queued = [e for e in _entries(redis_client, stream_key()) if e.get("job_id") == sub.job_id]
    assert queued
    hop("queue_publication (stream entry)", queued[0]["media_evidence_id"])

    worker = Worker(redis_client, block_ms=200, lease_ms=5000)
    entry = worker.claim_one()
    hop("queue_consumption (claimed job)", entry.job.media_evidence_id)

    inner = build_matching_handler(MediaAcquirer(connect_timeout_s=5, read_timeout_s=5, allow_private_networks=True), engine, registry)

    def spy(job):
        hop("processing (handler input: acquisition -> segments -> embedding -> candidate search -> match)", job.media_evidence_id)
        return inner(job)

    worker.process_claim(entry, spy)
    h = dict(redis_client.hgetall(result_key(sub.job_id)))
    hop("result_publication (result hash)", h["media_evidence_id"])
    ev = [e for e in _entries(redis_client, results_stream_key()) if e.get("job_id") == sub.job_id]
    assert ev
    hop("result_publication (results stream)", ev[0]["media_evidence_id"])
    view = resolve_outcome(redis_client, sub.job_id)
    assert view.outcome == FingerprintOutcome.MATCH
    hop("evidence_finalisation (resolve_outcome view)", view.media_evidence_id)
    if os.environ.get("FP09_TRACE_OUT"):
        Path(os.environ["FP09_TRACE_OUT"]).write_text(json.dumps({"scenario": "success", "trace": trace}, indent=1))


def test_v1_correlation_id_survives_retry_and_failure(redis_client, engine, registry):
    cid = f"fp09-retry-{uuid.uuid4()}"
    registry.register_target("target-1", "v1", str(TINY))
    sub = FingerprintJobSubmitter(redis_client).submit(FingerprintCandidate(
        candidate_url="http://127.0.0.1:1/never.mp4", media_evidence_id=cid, media_type="video", source_domain="synthetic.invalid",
        target_id="target-1", target_version="v1"))
    seen = []
    handler = build_matching_handler(MediaAcquirer(connect_timeout_s=1, read_timeout_s=1, allow_private_networks=True), engine, registry)
    w = Worker(redis_client, block_ms=200, lease_ms=5000)
    e = w.claim_one()
    seen.append(("attempt1", e.job.media_evidence_id))
    w.process_claim(e, handler)
    view = resolve_outcome(redis_client, sub.job_id)
    assert view.outcome == FingerprintOutcome.RETRYABLE_ERROR  # transient failure -> retry scheduled
    assert view.media_evidence_id in (cid, None)
    # the scheduled retry re-enters the stream with the same identifier
    retried = [x for x in _entries(redis_client, stream_key()) if x.get("job_id") == sub.job_id]
    assert retried and all(x["media_evidence_id"] == cid for x in retried)
    res = redis_client.hgetall(result_key(sub.job_id))
    if res:
        assert res.get("media_evidence_id") == cid
    ev = [x for x in _entries(redis_client, results_stream_key()) if x.get("job_id") == sub.job_id]
    assert all(x["media_evidence_id"] == cid for x in ev)


@pytest.mark.xfail(strict=True, reason="FP-09 defect: V2 events omit correlation_id/causation_id (matching.py match.found; requests.py encode.failed/ready "
                                       "pass causation only and never read the inbound envelope's correlation_id)")
def test_v2_outbound_events_carry_inbound_correlation_id():
    import inspect
    import fingerprinter.matching as matching
    import fingerprinter.requests as requests
    src = inspect.getsource(matching) + inspect.getsource(requests)
    assert "correlation_id=" in src
