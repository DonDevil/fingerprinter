"""P10 audit: copy the SOAK-1000 run-5 representations (``fp-seg-npz/v1``) into the local vector cache.

SOAK-1000 run 5 (P9 validation §8.4) left its artifacts in the dev MinIO
bucket ``fp-soak-<tag>-reps`` (its Scylla keyspace was dropped). They are the
production output of spec ``dinov2-seg/2`` (FP32), so they are real vectors
that need no re-encoding. Each artifact's SHA-256 is checked against the
``manifest.json`` inside it and the content is mapped back to its SOAK-1000
item through ``ContentId`` (``full-bytes/v1`` SHA-256 of the file). Read-only
on MinIO; the cache lives outside git.

    env/bin/python benchmarks/p10-index/fetch_soak_vectors.py --bucket fp-soak-cfb53dee-reps \
        --keys keys.txt --manifest ~/anti_piracy/datasets/p9/soak-1000/manifest.json \
        --out ~/anti_piracy/datasets/p10/vectors/soak-1000
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from pathlib import Path

import numpy as np

ROOT = Path(__file__).resolve().parents[2]


def main() -> None:
    sys.path.insert(0, str(ROOT))
    from minio import Minio

    from antipiracy_contracts.digests import ContentDigest
    from antipiracy_contracts.ids import ContentId
    from fingerprinter.artifact import load

    ap = argparse.ArgumentParser()
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--keys", type=Path, required=True, help="one object key per line")
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--out", type=Path, required=True)
    a = ap.parse_args()

    m = json.loads(a.manifest.read_text())
    by_content: dict[str, dict] = {}
    for it in m["items"]:
        if it.get("sha256"):
            by_content[str(ContentId.of("full-bytes/v1", ContentDigest("sha256:" + it["sha256"])))] = it
    c = Minio(os.environ["FINGERPRINTER_MINIO__ENDPOINT"], access_key=os.environ["FINGERPRINTER_MINIO__ACCESS_KEY"],
              secret_key=os.environ["FINGERPRINTER_MINIO__SECRET_KEY"], secure=False)
    a.out.mkdir(parents=True, exist_ok=True)
    index: dict = {"bucket": a.bucket, "manifest_sha256": hashlib.sha256(a.manifest.read_bytes()).hexdigest(),
                   "items": {}, "unmapped": []}
    for key in a.keys.read_text().split():
        r = c.get_object(a.bucket, key)
        try:
            data = r.read()
        finally:
            r.close()
            r.release_conn()
        arrays, man = load(data)
        cid = man["content_id"]
        it = by_content.get(cid)
        if it is None:
            index["unmapped"].append(cid)
            continue
        name = it["file"].replace("/", "__")
        np.savez(a.out / f"{name}.npz", segment_index=arrays["segment_index"], pts_s=arrays["pts_s"],
                 vectors=arrays["vectors"])
        index["items"][it["file"]] = {"content_id": cid, "segments": int(len(arrays["segment_index"])),
                                      "spec_digest": man.get("spec_digest"), "artifact_sha256":
                                      hashlib.sha256(data).hexdigest(), "films": it.get("films", [])}
    (a.out / "index.json").write_text(json.dumps(index, indent=1, sort_keys=True))
    print(json.dumps({"items": len(index["items"]), "unmapped": len(index["unmapped"]),
                      "segments": sum(v["segments"] for v in index["items"].values())}))


if __name__ == "__main__":
    main()
