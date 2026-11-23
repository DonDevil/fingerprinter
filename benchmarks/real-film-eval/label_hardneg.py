"""Apply reviewer labels to the mined hard-negative candidates. Only pairs whose contact sheet was actually viewed are
'confirmed'; everything else stays 'uncertain' (never silently a negative). Reviewer: Claude (visual review, 2026-11-23);
owner re-verification of the 'confirmed' set is still required before it is used for calibration (decision O-3)."""
from common import d, jdump, jload

REVIEWED = {  # pair index ranges viewed on contact sheets 00, 03, 05, 07 -> class
    range(0, 5): "credits roll (low-information) - different films, different names/layout",
    range(15, 20): "credits roll (low-information) - different films, different names/layout",
    range(25, 30): "credits/logos/title cards (low-information) - different films",
    range(35, 40): "night car interior - different scenes of different films (scene look-alike)",
}


def run() -> None:
    rows = jload(d("pairs", "hard-negatives", "candidates.json"))
    for r in rows:
        n = int(r["pair_id"].split("_")[1])
        cls = next((c for rg, c in REVIEWED.items() if n in rg), None)
        if cls:
            r.update(proposed_label="hard_negative", verification_status="confirmed_by_visual_review", reason=cls)
        elif n < 40:
            r.update(proposed_label="hard_negative_candidate", verification_status="uncertain_contact_sheet_not_viewed",
                     reason="different films by construction; sheet generated but not viewed")
        else:
            r.update(verification_status="uncertain_not_reviewed", reason="different films by construction; no visual review (no sheet)")
    jdump(rows, d("pairs", "hard-negatives", "candidates_labelled.json"))
    from collections import Counter
    print(Counter(r["verification_status"] for r in rows))
    conf = [r for r in rows if r["verification_status"].startswith("confirmed")]
    print("confirmed:", len(conf), "matcher-confirmed (false positives) among them @1fps:",
          sum(1 for r in conf if (r.get("matcher_1fps") or {}).get("verdict") == "confirmed"), "@0.2fps:",
          sum(1 for r in conf if (r.get("matcher_0.2fps") or {}).get("verdict") == "confirmed"))
    allfp = [r for r in rows if (r.get("matcher_0.2fps") or {}).get("verdict")]
    print("all candidates with a production-param (0.2fps) verdict:", len(allfp))


if __name__ == "__main__":
    run()
