"""Technique versions, calibration and ``confidence`` (design §9.1-§9.2; P10-D9, D10).

``technique = dinov2-seg-temporal / 2.<n>`` names an **immutable**
``verifier_params`` row (parameter document + digest), inserted once with
``IF NOT EXISTS``; any decision-affecting change is a new version and hence a
new ``match_id``. The row also carries the calibration record and its status
(``CALIBRATED`` / ``UNAVAILABLE`` / ``STALE``). ``confidence`` is the
calibrated probability that a proposed, verified match is correct — never a
similarity: without a ``CALIBRATED`` status there is no confidence and
nothing is published (decisions are stored and held).
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import numpy as np

from fingerprinter.db import SERIAL
from fingerprinter.p10ctx import P10
from fingerprinter.verify import TECHNIQUE_NAME, VerifierParams

CALIBRATED, UNAVAILABLE, STALE = "CALIBRATED", "UNAVAILABLE", "STALE"


class TechniqueError(Exception):
    pass


@dataclass(frozen=True)
class Technique:
    name: str
    version: str
    params: VerifierParams
    params_digest: str
    status: str
    calibration_id: str | None
    calibration: dict[str, Any] | None

    def confidence(self, score: float) -> float | None:
        """g(score) for a CALIBRATED technique; None otherwise (no probability is fabricated)."""
        if self.status != CALIBRATED or not self.calibration:
            return None
        model = self.calibration.get("model") or {}
        method = self.calibration.get("method")
        if method == "isotonic":
            lo, val = np.asarray(model["lo"]), np.asarray(model["val"])
            idx = int(np.clip(np.searchsorted(lo, score, side="right") - 1, 0, len(val) - 1))
            return float(min(max(val[idx], 0.0), 1.0))
        if method == "platt":
            return float(1.0 / (1.0 + np.exp(model["a"] * score + model["b"])))
        return None


class Techniques:
    def __init__(self, p10: P10) -> None:
        self.p = p10
        self.db = p10.db
        self._cache: dict[str, Technique] = {}

    def register(self, version: str, params: VerifierParams, *, calibration: dict[str, Any] | None, status: str,
                 calibration_id: str | None, actor: str, evidence: str) -> bool:
        if status not in (CALIBRATED, UNAVAILABLE, STALE):
            raise TechniqueError(f"unknown calibration status {status}")
        if status == CALIBRATED and not calibration:
            raise TechniqueError("CALIBRATED needs a calibration record")
        now = self.p.now()
        res = self.db.lwt(
            "INSERT INTO {ks}.verifier_params (technique_name, technique_version, params, params_digest, calibration, "
            "calibration_status, calibration_id, created_at, audit) VALUES (?,?,?,?,?,?,?,?,?) IF NOT EXISTS",
            TECHNIQUE_NAME, version, json.dumps(params.document(), sort_keys=True), params.digest(),
            json.dumps(calibration, sort_keys=True) if calibration else None, status, calibration_id, now,
            [json.dumps({"at": now.isoformat(), "actor": actor, "action": "register", "evidence": evidence})])
        if not res.applied and res.row.get("params_digest") != params.digest():
            raise TechniqueError(f"{TECHNIQUE_NAME} {version} exists with different parameters (immutable)")
        self._cache.pop(version, None)
        return res.applied

    def get(self, version: str) -> Technique:
        t = self._cache.get(version)
        if t is None:
            row = self.db.one("SELECT * FROM {ks}.verifier_params WHERE technique_name = ? AND technique_version = ?",
                              TECHNIQUE_NAME, version, level=SERIAL)
            if row is None:
                raise TechniqueError(f"technique {TECHNIQUE_NAME} {version} is not registered (match params register)")
            params = VerifierParams.from_document(json.loads(row["params"]))
            if params.digest() != row["params_digest"]:
                raise TechniqueError("stored verifier parameters do not hash to their digest")
            t = Technique(TECHNIQUE_NAME, version, params, row["params_digest"], row["calibration_status"],
                          row.get("calibration_id"), json.loads(row["calibration"]) if row.get("calibration") else None)
            self._cache[version] = t
        return t

    def forget(self) -> None:
        self._cache.clear()

    def set_calibration(self, version: str, *, status: str, calibration: dict[str, Any] | None,
                        calibration_id: str | None, actor: str, expect_status: str) -> bool:
        """Audited change of the calibration record (parameters stay immutable)."""
        now = self.p.now()
        ok = self.db.lwt("UPDATE {ks}.verifier_params SET calibration_status = ?, calibration = ?, calibration_id = ?, "
                         "audit = audit + ? WHERE technique_name = ? AND technique_version = ? "
                         "IF calibration_status = ?", status, json.dumps(calibration, sort_keys=True) if calibration
                         else None, calibration_id,
                         [json.dumps({"at": now.isoformat(), "actor": actor, "action": "calibration",
                                      "from": expect_status, "to": status})], TECHNIQUE_NAME, version,
                         expect_status).applied
        self._cache.pop(version, None)
        return ok


def params_from_tune_eval(report: dict[str, Any]) -> tuple[VerifierParams, dict[str, Any], str]:
    """(params, calibration record, status) of the final parameter set of ``tune_eval.py``."""
    fp = report["final_parameter_set"]
    params = VerifierParams.from_document(fp["params"])
    if params.digest() != fp["params_digest"]:
        raise TechniqueError("tune-eval parameter digest mismatch")
    cal = report["calibration"]
    record = {"method": cal["final_method"], "model": cal.get("final_model"), "data": cal["final_data"],
              "reliability": cal["outer_reliability"], "score_definition": params.score_definition,
              "candidate_policy": report["candidate_generation"]["policy"],
              "manifests_sha256": report.get("manifests_sha256")}
    return params, record, cal["status"]
