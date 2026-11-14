"""Shared P10 evaluation helpers: labelled populations, film-grouped folds, statistics.

Populations (validation.md §1):

- **L-PAIRS** — 7 targets, 180 clips, 480 (clip, target) pairs labelled by
  provenance (``l-pairs.csv``).
- **N-SOAK** — derived here from SOAK-1000 provenance, no new media: an item
  is *relevant* to a film when its reel interval overlaps the film's by
  >= 30 s, *irrelevant* when the intervals are >= 15 s apart, excluded
  otherwise (audit §9 rule). Synthetic / audio-only / corrupt items have no
  vectors and are skipped.

Folds (validation.md §3.1): the grouping unit is the source film. An L-PAIRS
clip belongs to its ``source_film``; a SOAK item to the film it overlaps
most (and its pairs with every other film it overlaps by >= 30 s are
excluded — "items spanning two films go to the fold of the film with the
larger overlap and are excluded from the other film's pairs"); items with
no film (synthetic) are distributed round-robin in file-name order.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import numpy as np

REL_MIN_OVERLAP_S = 30.0
IRREL_MIN_GAP_S = 15.0
SEGMENT_S = 5.0


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def wilson(k: int, n: int, z: float = 1.959964) -> tuple[float | None, float | None]:
    if n == 0:
        return None, None
    p = k / n
    den = 1 + z * z / n
    centre = (p + z * z / (2 * n)) / den
    half = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / den
    return round(max(0.0, centre - half), 4), round(min(1.0, centre + half), 4)


def confusion(rows: list[tuple[bool, bool]]) -> dict:
    """rows: (label_positive, predicted_positive)."""
    tp = sum(1 for y, p in rows if y and p)
    fn = sum(1 for y, p in rows if y and not p)
    fp = sum(1 for y, p in rows if not y and p)
    tn = sum(1 for y, p in rows if not y and not p)
    prec = tp / (tp + fp) if tp + fp else None
    rec = tp / (tp + fn) if tp + fn else None
    return {
        "tp": tp, "fn": fn, "fp": fp, "tn": tn,
        "precision": None if prec is None else round(prec, 4),
        "recall": None if rec is None else round(rec, 4),
        "precision_wilson95": wilson(tp, tp + fp),
        "recall_wilson95": wilson(tp, tp + fn),
    }


@dataclass
class Item:
    name: str          # unique key (file path inside its corpus)
    corpus: str        # "l-pairs" | "soak"
    fold: str          # film id of the fold
    transform: str | None
    vfr: bool = False
    films: list[str] = field(default_factory=list)


@dataclass
class Pair:
    item: str
    target: str        # film id
    label: bool
    corpus: str
    fold: str
    transform: str | None


@dataclass
class Population:
    films: list[str]                         # film ids in reel order
    target_file: dict[str, str]              # film id -> L-PAIRS target file name
    items: dict[str, Item]
    pairs: list[Pair]
    manifests: dict[str, str]                # name -> sha256 of the manifest / csv


def _round_robin(names: list[str], films: list[str]) -> dict[str, str]:
    return {n: films[i % len(films)] for i, n in enumerate(sorted(names))}


def load_lpairs(data: Path) -> Population:
    corpus = data / "l-pairs"
    manifest = json.loads((corpus / "manifest.json").read_text())
    reel = json.loads((data / "reel" / "manifest.json").read_text())
    films = [f["id"] for f in reel["films"]]
    target_file: dict[str, str] = {}
    items: dict[str, Item] = {}
    synthetic = []
    for it in manifest["items"]:
        name = Path(it["file"]).name
        if it["kind"] == "target":
            target_file[it["film"]] = name
            continue
        if it.get("source_film"):
            items[name] = Item(name, "l-pairs", it["source_film"], it.get("transform"), films=it["films"])
        else:
            synthetic.append(name)
            items[name] = Item(name, "l-pairs", "", it.get("transform"), films=[])
    for name, film in _round_robin(synthetic, films).items():
        items[name].fold = film
    by_file = {v: k for k, v in target_file.items()}
    pairs = []
    for r in csv.DictReader((corpus / "l-pairs.csv").open()):
        it = items[r["clip"]]
        pairs.append(Pair(r["clip"], by_file[r["target"]], r["label"] == "1", "l-pairs", it.fold, r["transform"]))
    return Population(
        films, target_file, items, pairs,
        {"l-pairs/manifest.json": sha256_file(corpus / "manifest.json"),
         "l-pairs/l-pairs.csv": sha256_file(corpus / "l-pairs.csv"),
         "reel/manifest.json": sha256_file(data / "reel" / "manifest.json")},
    )


def load_nsoak(data: Path, vectors: Path) -> Population:
    """N-SOAK pairs (module docstring). Only items with cached vectors count."""
    reel = json.loads((data / "reel" / "manifest.json").read_text())
    films = [f["id"] for f in reel["films"]]
    span = {f["id"]: (f["reel_start_s"], f["reel_start_s"] + f["duration_s"]) for f in reel["films"]}
    manifest = json.loads((data / "soak-1000" / "manifest.json").read_text())
    items: dict[str, Item] = {}
    pairs: list[Pair] = []
    no_film = []
    for it in manifest["items"]:
        if it["kind"] != "video":
            continue
        name = it["file"]
        if not (vectors / "soak-1000" / (name.replace("/", "__") + ".npz")).exists():
            continue
        a, b = it["reel_start_s"], it["reel_start_s"] + it["seconds"]
        overlap = {f: max(0.0, min(b, span[f][1]) - max(a, span[f][0])) for f in films}
        gap = {f: max(span[f][0] - b, a - span[f][1], 0.0) for f in films}
        best = max(films, key=lambda f: (overlap[f], -films.index(f)))
        item = Item(name, "soak", best if overlap[best] > 0 else "", None, bool(it.get("vfr")), it.get("films") or [])
        items[name] = item
        if overlap[best] <= 0:
            no_film.append(name)
        for f in films:
            if overlap[f] >= REL_MIN_OVERLAP_S:
                if f == best:
                    pairs.append(Pair(name, f, True, "soak", "", None))
                # relevant to a second film: excluded from that film's pairs (fold rule)
            elif overlap[f] == 0 and gap[f] >= IRREL_MIN_GAP_S:
                pairs.append(Pair(name, f, False, "soak", "", None))
    for name, film in _round_robin(no_film, films).items():
        items[name].fold = film
    for p in pairs:
        p.fold = items[p.item].fold
    return Population(
        films, {}, items, pairs,
        {"soak-1000/manifest.json": sha256_file(data / "soak-1000" / "manifest.json"),
         "reel/manifest.json": sha256_file(data / "reel" / "manifest.json")},
    )


@dataclass
class Rep:
    """One cached ``dinov2-seg/2`` representation (the arrays of ``fp-seg-npz/v1`` used by matching)."""

    segment_index: np.ndarray
    pts_s: np.ndarray
    vectors: np.ndarray


def load_vectors(vectors: Path, pop_lp: Population | None, pop_soak: Population | None) -> dict[str, Rep]:
    """Cached vectors keyed like the populations: L-PAIRS clip/target file name, SOAK item path,
    and ``film:<id>`` for the 7 targets."""
    out: dict[str, Rep] = {}

    def rep(path: Path) -> Rep:
        z = np.load(path)
        return Rep(z["segment_index"].astype(np.int64), z["pts_s"].astype(np.float64),
                   np.ascontiguousarray(z["vectors"].astype(np.float32)))

    if pop_lp is not None:
        for name in pop_lp.items:
            out[name] = rep(vectors / "l-pairs" / f"clips__{name}.npz")
        for film, fname in pop_lp.target_file.items():
            out["film:" + film] = rep(vectors / "l-pairs" / f"targets__{fname}.npz")
    if pop_soak is not None:
        for name in pop_soak.items:
            out[name] = rep(vectors / "soak-1000" / (name.replace("/", "__") + ".npz"))
    return out


def by_fold(pairs: list[Pair]) -> dict[str, list[Pair]]:
    out: dict[str, list[Pair]] = defaultdict(list)
    for p in pairs:
        out[p.fold].append(p)
    return out
