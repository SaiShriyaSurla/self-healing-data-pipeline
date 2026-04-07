from __future__ import annotations

from pathlib import Path

import pandas as pd

from features import load_and_prepare


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def choose_best_value(primary: object, secondary: object) -> object:
    if isinstance(primary, str) and primary.strip():
        return primary
    if isinstance(secondary, str) and secondary.strip():
        return secondary
    return primary if pd.notna(primary) else secondary


def merge_auto_matches(
    source_a: pd.DataFrame,
    source_b: pd.DataFrame,
    matches: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    auto_matches = matches[matches["match_status"] == "auto_match"].copy()
    review_matches = matches[matches["match_status"] == "review"].copy()
    auto_matches = auto_matches.sort_values(
        ["member_id", "match_score", "patient_id"],
        ascending=[True, False, True],
    ).drop_duplicates(subset=["member_id"], keep="first")

    source_a_indexed = source_a.set_index("patient_id")
    source_b_indexed = source_b.set_index("member_id")

    merged_rows: list[dict[str, object]] = []

    for _, match in auto_matches.iterrows():
        a = source_a_indexed.loc[match["patient_id"]]
        b = source_b_indexed.loc[match["member_id"]]

        merged_rows.append(
            {
                "unified_patient_id": f"UNI-{len(merged_rows) + 1:05d}",
                "source_a_patient_id": match["patient_id"],
                "source_b_member_id": match["member_id"],
                "full_name": choose_best_value(a["full_name"], b["full_name"]),
                "date_of_birth": choose_best_value(a["normalized_dob"], b["normalized_dob"]),
                "email": choose_best_value(a["normalized_email"], b["normalized_email"]),
                "phone": choose_best_value(a["normalized_phone"], b["normalized_phone"]),
                "address": choose_best_value(a["normalized_address"], b["normalized_address"]),
                "city": choose_best_value(a.get("city", ""), b.get("city_name", "")),
                "state": choose_best_value(a.get("state", ""), b.get("state_code", "")),
                "zip_code": choose_best_value(a.get("zip_code", ""), b.get("postal_code", "")),
                "match_score": match["match_score"],
                "match_status": match["match_status"],
            }
        )

    merged_df = pd.DataFrame(merged_rows)
    return merged_df, review_matches


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    source_a, source_b = load_and_prepare()
    matches = pd.read_csv(PROCESSED_DIR / "match_candidates.csv")

    merged_df, review_df = merge_auto_matches(source_a, source_b, matches)

    merged_df.to_csv(PROCESSED_DIR / "unified_patients.csv", index=False)
    review_df.to_csv(PROCESSED_DIR / "manual_review.csv", index=False)

    print("Unified patients created:", len(merged_df))
    print("Manual review records:", len(review_df))
    print("\nUnified patient sample:")
    print(merged_df.head(10).to_string(index=False))


if __name__ == "__main__":
    main()
