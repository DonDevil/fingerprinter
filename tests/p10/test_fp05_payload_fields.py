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


def test_candidate_search_pushes_the_policy_floor_into_qdrant_and_keeps_recall():
    """FP-05 (step 2): the search carries ``score_threshold=floor`` so Qdrant reads no payload for hits the
    grouping would discard; the hits at/above the floor, and the escalation to a larger k, are unchanged."""
    import time
    from types import SimpleNamespace

    from fingerprinter.candidates import Policy, Searcher
    from fingerprinter.generations import Pointer
    from fingerprinter.vindex import Hit
    from fingerprinter.verify import Vectors

    sent: list[SearchRequest] = []

    def search_batch(name, reqs, **kw):
        sent.extend(reqs)
        # emulate Qdrant: scores >= threshold only, whole payload of the requested keys
        pool = [Hit(f"p{i}", 0.95 - 0.05 * i, {"spec": "S", "trep": f"t{i % 3}", "cid": f"c{i % 3}", "rep": "r", "tid": "x",
                                              "tver": 1, "seq": 1, "k": i, "lowinfo": False}) for i in range(10)]
        return [[h for h in pool if h.score >= r.score_threshold][: r.limit] for r in reqs]

    cfg = SimpleNamespace(matching=SimpleNamespace(search_timeout_s=30.0), index=SimpleNamespace(search_ef=None,
                                                                                               oversampling=2.0))
    ptr = Pointer(1, "col", 1, None, time.monotonic())  # type: ignore[arg-type]
    gens = SimpleNamespace(active=lambda *a, **k: ptr)
    p10 = SimpleNamespace(settings=cfg, index=SimpleNamespace(search_batch=search_batch), hook=lambda *a, **k: None)
    q = Vectors(np.array([0]), np.array([0.0]), np.zeros((1, 4), np.float32))
    pol = Policy(k0=4, k_max=16, floor=0.70, min_hits=1, cap=10)
    out = Searcher(p10, gens).search("S", "target_vs_media", q, pol, skip_query=set(), exclude_key=None,  # type: ignore[arg-type]
                                     hit_excluded=None)
    assert {r.score_threshold for r in sent} == {0.70}
    # scores >= 0.70 are 0.95..0.70 -> 6 hits; k0=4 saturates (4 >= k and last >= floor) -> escalates to k=8
    assert [r.limit for r in sent] == [4, 8]
    assert out.stats["hits"] == 6 and out.stats["hits_above_floor"] == 6
    assert {g.key for g in out.candidates} == {"c0", "c1", "c2"}
