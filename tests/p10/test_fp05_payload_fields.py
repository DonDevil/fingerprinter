"""FP-05: search responses carry only the payload keys the consumers read (stored payload untouched)."""

from __future__ import annotations

import numpy as np

from fingerprinter.candidates import CANDIDATE_PAYLOAD_FIELDS
from fingerprinter.lowinfo import LOWINFO_WITNESS_FIELDS
from fingerprinter.vindex import QdrantIndex, SearchRequest


def _index(captured: list[dict]) -> QdrantIndex:
    ix = QdrantIndex("http://unused", None)

    def fake_call(method, path, body=None, **kw):
        captured.append(body)
        return [[{"id": "a", "score": 0.9, "payload": {"k": 1}},      # a field requested but absent -> tolerated
                 {"id": "b", "score": 0.8}]]                         # no payload key at all -> {}

    ix._call = fake_call  # type: ignore[method-assign]
    return ix


def test_default_request_still_returns_whole_payload():
    sent: list[dict] = []
    _index(sent).search_batch("c", [SearchRequest(vector=np.zeros(4, np.float32), limit=3)],
                              hnsw_ef=None, oversampling=1.0)
    assert sent[0]["searches"][0]["with_payload"] is True


def test_payload_fields_are_sent_as_an_explicit_list_and_missing_keys_are_safe():
    sent: list[dict] = []
    hits = _index(sent).search_batch(
        "c", [SearchRequest(vector=np.zeros(4, np.float32), limit=3, payload_fields=("k", "trep"))],
        hnsw_ef=None, oversampling=1.0)[0]
    assert sent[0]["searches"][0]["with_payload"] == ["k", "trep"]
    assert hits[0].payload == {"k": 1} and hits[1].payload == {}


def test_field_sets_cover_every_key_the_consumers_read_and_drop_the_unused_ones():
    assert {"spec", "trep", "cid", "tid", "tver", "seq", "k", "rep", "lowinfo"} == set(CANDIDATE_PAYLOAD_FIELDS)
    assert not {"t", "lowinfo_rev"} & set(CANDIDATE_PAYLOAD_FIELDS)
    assert {"tid", "trep", "tver", "k"} == set(LOWINFO_WITNESS_FIELDS)
