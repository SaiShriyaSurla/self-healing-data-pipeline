from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from features import load_and_prepare
from match import build_candidate_pairs
from merge import merge_auto_matches
from report import build_report
from validate import validate_sources


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def run_pipeline() -> dict[str, object]:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    validation_report = validate_sources()

    source_a, source_b = load_and_prepare()

    matches = build_candidate_pairs(source_a, source_b)
    matches_path = PROCESSED_DIR / "match_candidates.csv"
    matches.to_csv(matches_path, index=False)

    merged_df, review_df = merge_auto_matches(source_a, source_b, matches)
    unified_path = PROCESSED_DIR / "unified_patients.csv"
    review_path = PROCESSED_DIR / "manual_review.csv"
    merged_df.to_csv(unified_path, index=False)
    review_df.to_csv(review_path, index=False)

    report = build_report()
    report_path = PROCESSED_DIR / "reconciliation_report.json"
    with report_path.open("w", encoding="utf-8") as file_handle:
        json.dump(report, file_handle, indent=2)

    summary = {
        "source_a_rows": validation_report["row_counts"]["patients_source_a"],
        "source_b_rows": validation_report["row_counts"]["patients_source_b"],
        "appointments_rows": validation_report["row_counts"]["appointments"],
        "auto_matched_records": int((matches["match_status"] == "auto_match").sum()),
        "review_records": int((matches["match_status"] == "review").sum()),
        "unified_patients_created": int(len(merged_df)),
        "manual_review_queue": int(len(review_df)),
        "output_files": [
            str(matches_path),
            str(unified_path),
            str(review_path),
            str(report_path),
        ],
    }

    return summary


def main() -> None:
    summary = run_pipeline()
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
