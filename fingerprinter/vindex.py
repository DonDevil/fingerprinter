"""The vector index adapter (P10 design §3, §4; ADR-028, P10-D1/D2/D4).

``VectorIndex`` is the narrow protocol the indexer, searcher, verifier passes
and the builder use; ``QdrantIndex`` implements it against Qdrant 1.15.1 over
REST. The index is **derived state**: nothing here is authoritative, and no
call is conditional (Qdrant has no conditional writes — design §12.2 says
how the protocol lives with that).

Errors are typed and never turned into empty results (design §11.2):

- ``IndexUnavailableError`` — connection failure, timeout, HTTP 429/5xx: the
  outcome of a write is unknown and the caller repeats the (idempotent)
  operation; a search did not answer.
- ``IndexMissingError`` — HTTP 404 for a collection: the caller decides from
  Scylla whether that is legitimate (RETIRED/FAILED generation) or an
  invariant violation.
- ``IndexPermanentError`` — any other 4xx (wrong dimension, invalid payload):
  not retried; the mutation row becomes BLOCKED.

Point ids (design §4.3): ``uuid5(NS, f"{subject}:{seq}:{segment_index}")``
with ``NS = uuid5(NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")``.
"""

from __future__ import annotations

import hashlib
import json
import time
import uuid
from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import numpy as np
import requests

from fingerprinter.errors import FingerprinterError

NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "antipiracy/fingerprinter/index/v1")
PAYLOAD_SCHEMA = "p10-points/1"
MEDIA, TARGET = "media", "target"
SUBJECT_FIELD = {MEDIA: "rep", TARGET: "trep"}
NORM_TOLERANCE = 1e-4


class IndexServiceError(FingerprinterError):
    """Base class of index errors."""


class IndexUnavailableError(IndexServiceError):
    """The index did not answer (or answered 429/5xx): retry with backoff."""


class IndexMissingError(IndexServiceError):
    """The collection does not exist."""


class IndexPermanentError(IndexServiceError):
    """A request the index rejects for good (4xx other than 404/409/429)."""


class InvalidVectorError(FingerprinterError):
    """A vector that the spec says is L2-normalized is not (design §4.2)."""


def point_id(subject: str, seq: int, segment_index: int) -> str:
    return str(uuid.uuid5(NAMESPACE, f"{subject}:{int(seq)}:{int(segment_index)}"))


def collection_name(spec_digest: str, role: str, generation: int, prefix: str = "fp") -> str:
    """``<prefix>_<spec12>_<role>_g<n>`` (design §4.2; ``prefix`` = ``index.collection_prefix``, N-14).

    The prefix namespaces one deployment (keyspace) inside a Qdrant instance: two keyspaces sharing
    an instance must never derive the same collection name."""
    hexd = spec_digest.split(":", 1)[-1]
    return f"{prefix}_{hexd[:12]}_{role}_g{int(generation)}"


@dataclass(frozen=True)
class GenerationParams:
    """Immutable per-generation parameters (design §12.1); their digest is provenance."""

    qdrant_version: str = "1.15.1"
    distance: str = "Dot"
    dim: int = 768
    hnsw_m: int = 32
    ef_construct: int = 200
    quantization: str = "int8"
    quantization_always_ram: bool = True
    vectors_on_disk: bool = True
    payload_on_disk: bool = True
    payload_schema: str = PAYLOAD_SCHEMA
    lowinfo_rule: str = "lowinfo/v1"

    def document(self) -> dict[str, Any]:
        return dict(sorted(self.__dict__.items()))

    def digest(self) -> str:
        return hashlib.sha256(json.dumps(self.document(), sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @classmethod
    def from_document(cls, doc: dict[str, Any]) -> GenerationParams:
        return cls(**doc)


@dataclass(frozen=True)
class Point:
    id: str
    vector: np.ndarray | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class Hit:
    id: str
    score: float
    payload: dict[str, Any]


@dataclass
class SearchRequest:
    vector: np.ndarray
    limit: int
    exact: bool = False
    ignore_quantization: bool = False
    score_threshold: float | None = None
    must_not: list[dict[str, Any]] = field(default_factory=list)


class VectorIndex(Protocol):
    def health(self) -> dict[str, Any]: ...
    def version(self) -> str: ...
    def create_collection(self, name: str, params: GenerationParams, role: str) -> None: ...
    def delete_collection(self, name: str) -> bool: ...
    def collection_exists(self, name: str) -> bool: ...
    def collection_info(self, name: str) -> dict[str, Any]: ...
    def collections(self) -> list[str]: ...
    def upsert(self, name: str, points: Sequence[Point]) -> None: ...
    def delete_by_filter(self, name: str, flt: dict[str, Any]) -> None: ...
    def delete_ids(self, name: str, ids: Sequence[str]) -> None: ...
    def count(self, name: str, flt: dict[str, Any] | None = None) -> int: ...
    def search_batch(self, name: str, requests_: Sequence[SearchRequest], *, hnsw_ef: int | None,
                     oversampling: float, timeout_s: float | None = None) -> list[list[Hit]]: ...
    def scroll(self, name: str, flt: dict[str, Any] | None = None, *, page: int = 256,
               with_vector: bool = False, offset: str | None = None) -> tuple[list[Point], str | None]: ...
    def retrieve(self, name: str, ids: Sequence[str], *, with_vector: bool = True) -> list[Point]: ...
    def set_alias(self, alias: str, name: str) -> None: ...


# --- filters (design §12.2: every removal is bounded by the operation's own seq) ---------


def subject_filter(role: str, subject: str) -> dict[str, Any]:
    return {"must": [{"key": SUBJECT_FIELD[role], "match": {"value": subject}}]}


def subject_seq_eq(role: str, subject: str, seq: int) -> dict[str, Any]:
    return {"must": [{"key": SUBJECT_FIELD[role], "match": {"value": subject}},
                     {"key": "seq", "match": {"value": int(seq)}}]}


def subject_seq_lt(role: str, subject: str, seq: int) -> dict[str, Any]:
    """Cleanup after UPSERT ``u``: ``subject = S AND seq < u``."""
    return {"must": [{"key": SUBJECT_FIELD[role], "match": {"value": subject}},
                     {"key": "seq", "range": {"lt": int(seq)}}]}


def subject_seq_lte(role: str, subject: str, seq: int) -> dict[str, Any]:
    """DELETE ``d``: ``subject = S AND seq <= d``."""
    return {"must": [{"key": SUBJECT_FIELD[role], "match": {"value": subject}},
                     {"key": "seq", "range": {"lte": int(seq)}}]}


def check_unit(vectors: np.ndarray) -> None:
    norms = np.linalg.norm(vectors.astype(np.float64), axis=1)
    bad = np.where(np.abs(norms - 1.0) > NORM_TOLERANCE)[0]
    if bad.size:
        raise InvalidVectorError(f"{bad.size} vector(s) not L2-normalized (first norm {norms[bad[0]]:.6f})")


class QdrantIndex:
    """REST adapter (Qdrant 1.15.1). Thread-safe for concurrent requests (one ``Session``)."""

    def __init__(self, url: str, api_key: str | None, *, timeout_s: float = 10.0) -> None:
        self.url = url.rstrip("/")
        self.timeout_s = timeout_s
        self._session = requests.Session()
        if api_key:
            self._session.headers["api-key"] = api_key
        self._session.headers["content-type"] = "application/json"
        # requests asks for gzip by default; Qdrant 1.15.1 then compresses search responses and the
        # small ones stall ≈ 40 ms each (validation §13.17: limit 10-24 at p50 42 ms, 0.8 ms with
        # identity). The index runs on loopback / a backend network, so compression buys nothing.
        self._session.headers["accept-encoding"] = "identity"

    # --- transport ------------------------------------------------------------

    def _call(self, method: str, path: str, body: Any = None, *, timeout_s: float | None = None,
              missing_ok: bool = False) -> Any:
        try:
            resp = self._session.request(method, self.url + path,
                                         data=None if body is None else json.dumps(body),
                                         timeout=timeout_s or self.timeout_s)
        except requests.RequestException as exc:
            raise IndexUnavailableError(f"{method} {path}: {type(exc).__name__}: {exc}") from exc
        if resp.status_code == 404:
            if missing_ok:
                return None
            raise IndexMissingError(f"{method} {path}: 404 {resp.text[:200]}")
        if resp.status_code == 429 or resp.status_code >= 500:
            raise IndexUnavailableError(f"{method} {path}: {resp.status_code} {resp.text[:200]}")
        if resp.status_code >= 400:
            # Qdrant reports "Collection `x` doesn't exist!" for some endpoints as 400/404
            if "doesn't exist" in resp.text or "Not found" in resp.text:
                if missing_ok:
                    return None
                raise IndexMissingError(f"{method} {path}: {resp.status_code} {resp.text[:200]}")
            raise IndexPermanentError(f"{method} {path}: {resp.status_code} {resp.text[:300]}")
        if not resp.content:
            return None
        doc = resp.json()
        return doc.get("result", doc) if isinstance(doc, dict) else doc

    # --- service --------------------------------------------------------------

    def health(self) -> dict[str, Any]:
        try:
            resp = self._session.get(self.url + "/readyz", timeout=self.timeout_s)
        except requests.RequestException as exc:
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        return {"ok": resp.status_code == 200, "status": resp.status_code}

    def version(self) -> str:
        return str(self._call("GET", "/").get("version"))

    # --- collections ----------------------------------------------------------

    def create_collection(self, name: str, params: GenerationParams, role: str) -> None:
        if params.distance != "Dot" or params.quantization != "int8":
            raise IndexPermanentError(f"unsupported generation params {params.document()}")
        body = {
            "vectors": {"size": params.dim, "distance": params.distance, "on_disk": params.vectors_on_disk},
            "hnsw_config": {"m": params.hnsw_m, "ef_construct": params.ef_construct},
            "quantization_config": {"scalar": {"type": "int8", "always_ram": params.quantization_always_ram}},
            "on_disk_payload": params.payload_on_disk,
        }
        self._call("PUT", f"/collections/{name}", body, timeout_s=60)
        keyword = ("rep", "cid", "spec") if role == MEDIA else ("rep", "trep", "cid", "tid", "spec")
        for fname in keyword:
            self._call("PUT", f"/collections/{name}/index?wait=true",
                       {"field_name": fname, "field_schema": "keyword"}, timeout_s=60)
        self._call("PUT", f"/collections/{name}/index?wait=true",
                   {"field_name": "seq", "field_schema": "integer"}, timeout_s=60)

    def delete_collection(self, name: str) -> bool:
        res = self._call("DELETE", f"/collections/{name}", timeout_s=120, missing_ok=True)
        return bool(res)

    def collection_exists(self, name: str) -> bool:
        res = self._call("GET", f"/collections/{name}/exists", missing_ok=True)
        return bool(res and res.get("exists"))

    def collection_info(self, name: str) -> dict[str, Any]:
        return dict(self._call("GET", f"/collections/{name}"))

    def collections(self) -> list[str]:
        return sorted(c["name"] for c in self._call("GET", "/collections")["collections"])

    def set_alias(self, alias: str, name: str) -> None:
        """Operator convenience only (design §4.2): never read by the matcher."""
        self._call("POST", "/collections/aliases", {"actions": [
            {"delete_alias": {"alias_name": alias}},
            {"create_alias": {"collection_name": name, "alias_name": alias}},
        ]})

    # --- points -------------------------------------------------------------------

    def upsert(self, name: str, points: Sequence[Point]) -> None:
        if not points:
            return
        body = {"points": [{"id": p.id, "vector": np.asarray(p.vector, dtype=np.float32).tolist(),
                            "payload": p.payload} for p in points]}
        self._call("PUT", f"/collections/{name}/points?wait=true", body, timeout_s=max(self.timeout_s, 60))

    def delete_by_filter(self, name: str, flt: dict[str, Any]) -> None:
        self._call("POST", f"/collections/{name}/points/delete?wait=true", {"filter": flt},
                   timeout_s=max(self.timeout_s, 60))

    def delete_ids(self, name: str, ids: Sequence[str]) -> None:
        if ids:
            self._call("POST", f"/collections/{name}/points/delete?wait=true", {"points": list(ids)},
                       timeout_s=max(self.timeout_s, 60))

    def count(self, name: str, flt: dict[str, Any] | None = None) -> int:
        body: dict[str, Any] = {"exact": True}
        if flt:
            body["filter"] = flt
        return int(self._call("POST", f"/collections/{name}/points/count", body)["count"])

    def search_batch(self, name: str, requests_: Sequence[SearchRequest], *, hnsw_ef: int | None,
                     oversampling: float, timeout_s: float | None = None) -> list[list[Hit]]:
        searches = []
        for r in requests_:
            params: dict[str, Any] = {"exact": bool(r.exact),
                                      "quantization": {"ignore": bool(r.ignore_quantization),
                                                       "rescore": True, "oversampling": oversampling}}
            if hnsw_ef is not None and not r.exact:
                params["hnsw_ef"] = int(hnsw_ef)
            s: dict[str, Any] = {"vector": np.asarray(r.vector, dtype=np.float32).tolist(),
                                 "limit": int(r.limit), "params": params, "with_payload": True}
            if r.score_threshold is not None:
                s["score_threshold"] = float(r.score_threshold)
            if r.must_not:
                s["filter"] = {"must_not": r.must_not}
            searches.append(s)
        out: list[list[Hit]] = []
        for i in range(0, len(searches), 64):
            res = self._call("POST", f"/collections/{name}/points/search/batch",
                             {"searches": searches[i:i + 64]}, timeout_s=timeout_s)
            for hits in res:
                out.append([Hit(str(h["id"]), float(h["score"]), h.get("payload") or {}) for h in hits])
        return out

    def scroll(self, name: str, flt: dict[str, Any] | None = None, *, page: int = 256,
               with_vector: bool = False, offset: str | None = None) -> tuple[list[Point], str | None]:
        body: dict[str, Any] = {"limit": int(page), "with_payload": True, "with_vector": with_vector}
        if flt:
            body["filter"] = flt
        if offset is not None:
            body["offset"] = offset
        res = self._call("POST", f"/collections/{name}/points/scroll", body, timeout_s=max(self.timeout_s, 60))
        pts = [Point(str(p["id"]), np.asarray(p["vector"], dtype=np.float32) if with_vector and p.get("vector")
                     is not None else None, p.get("payload") or {}) for p in res["points"]]
        nxt = res.get("next_page_offset")
        return pts, None if nxt is None else str(nxt)

    def scroll_all(self, name: str, flt: dict[str, Any] | None = None, *, page: int = 256,
                   with_vector: bool = False) -> Iterator[Point]:
        offset = None
        while True:
            pts, offset = self.scroll(name, flt, page=page, with_vector=with_vector, offset=offset)
            yield from pts
            if offset is None:
                return

    def retrieve(self, name: str, ids: Sequence[str], *, with_vector: bool = True) -> list[Point]:
        res = self._call("POST", f"/collections/{name}/points",
                         {"ids": list(ids), "with_payload": True, "with_vector": with_vector})
        return [Point(str(p["id"]), np.asarray(p["vector"], dtype=np.float32) if with_vector else None,
                      p.get("payload") or {}) for p in res]

    def wait_green(self, name: str, timeout_s: float = 600.0) -> str:
        deadline = time.monotonic() + timeout_s
        while True:
            status = self.collection_info(name).get("status")
            if status == "green" or time.monotonic() > deadline:
                return str(status)
            time.sleep(0.5)
