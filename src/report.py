from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from validate import validate_sources


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def build_report() -> dict[str, object]:
    validation_report = validate_sources()
    matches = pd.read_csv(PROCESSED_DIR / "match_candidates.csv")
    unified = pd.read_csv(PROCESSED_DIR / "unified_patients.csv")
    manual_review = pd.read_csv(PROCESSED_DIR / "manual_review.csv")

    report = {
        "project": "Self-Healing Data Pipeline with AI Reconciliation",
        "summary": {
            "source_a_rows": validation_report["row_counts"]["patients_source_a"],
            "source_b_rows": validation_report["row_counts"]["patients_source_b"],
            "appointments_rows": validation_report["row_counts"]["appointments"],
            "auto_matched_records": int((matches["match_status"] == "auto_match").sum()),
            "review_records": int((matches["match_status"] == "review").sum()),
            "unified_patients_created": int(len(unified)),
            "manual_review_queue": int(len(manual_review)),
        },
        "data_quality": {
            "source_b_missing_email": validation_report["nulls"]["patients_source_b"].get("email_address", 0),
            "source_b_missing_postal_code": validation_report["nulls"]["patients_source_b"].get("postal_code", 0),
            "appointments_blank_patient_ref": validation_report["broken_joins"]["blank_patient_ref_count"],
            "appointments_orphan_patient_ref": validation_report["broken_joins"]["orphan_patient_ref_count"],
            "source_a_duplicate_rows": validation_report["duplicates"]["patients_source_a"]["duplicate_row_count"],
            "source_b_duplicate_rows": validation_report["duplicates"]["patients_source_b"]["duplicate_row_count"],
        },
        "samples": {
            "manual_review_examples": manual_review.head(10).to_dict(orient="records"),
            "unified_patient_examples": unified.head(10).to_dict(orient="records"),
        },
    }

    return report


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    report = build_report()
    report_path = PROCESSED_DIR / "reconciliation_report.json"

    with report_path.open("w", encoding="utf-8") as file_handle:
        json.dump(report, file_handle, indent=2)

    print(json.dumps(report["summary"], indent=2))
    print("\nReport saved to:", report_path)


if __name__ == "__main__":
    main()
