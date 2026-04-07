from __future__ import annotations

from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from features import load_and_prepare


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"


def similarity_score(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return fuzz.ratio(left, right) / 100.0


def compute_match_score(a_row: pd.Series, b_row: pd.Series) -> dict[str, float]:
    name_score = similarity_score(a_row["normalized_name"], b_row["normalized_name"])
    address_score = similarity_score(a_row["normalized_address"], b_row["normalized_address"])

    email_match = float(
        bool(a_row["normalized_email"])
        and bool(b_row["normalized_email"])
        and a_row["normalized_email"] == b_row["normalized_email"]
    )
    phone_match = float(
        bool(a_row["normalized_phone"])
        and bool(b_row["normalized_phone"])
        and a_row["normalized_phone"] == b_row["normalized_phone"]
    )
    dob_match = float(
        bool(a_row["normalized_dob"])
        and bool(b_row["normalized_dob"])
        and a_row["normalized_dob"] == b_row["normalized_dob"]
    )

    weighted_score = (
        name_score * 0.30
        + address_score * 0.15
        + email_match * 0.20
        + phone_match * 0.20
        + dob_match * 0.15
    )

    return {
        "name_score": round(name_score, 3),
        "address_score": round(address_score, 3),
        "email_match": email_match,
        "phone_match": phone_match,
        "dob_match": dob_match,
        "match_score": round(weighted_score, 3),
    }


def classify_match(score: float) -> str:
    if score >= 0.85:
        return "auto_match"
    if score >= 0.65:
        return "review"
    return "no_match"


def build_candidate_pairs(source_a: pd.DataFrame, source_b: pd.DataFrame) -> pd.DataFrame:
    pairs: list[dict[str, object]] = []

    for _, a_row in source_a.iterrows():
        candidates = source_b[
            (source_b["normalized_dob"] == a_row["normalized_dob"])
            | (source_b["normalized_phone"] == a_row["normalized_phone"])
            | (source_b["normalized_email"] == a_row["normalized_email"])
        ].copy()

        if candidates.empty:
            continue

        for _, b_row in candidates.iterrows():
            scores = compute_match_score(a_row, b_row)

            pair = {
                "patient_id": a_row["patient_id"],
                "member_id": b_row["member_id"],
                "source_a_name": a_row["full_name"],
                "source_b_name": b_row["full_name"],
                "source_a_email": a_row["normalized_email"],
                "source_b_email": b_row["normalized_email"],
                "source_a_phone": a_row["normalized_phone"],
                "source_b_phone": b_row["normalized_phone"],
                "source_a_dob": a_row["normalized_dob"],
                "source_b_dob": b_row["normalized_dob"],
                **scores,
            }
            pair["match_status"] = classify_match(pair["match_score"])
            pairs.append(pair)

    if not pairs:
        return pd.DataFrame()

    matches = pd.DataFrame(pairs)
    matches = matches.sort_values(["patient_id", "match_score"], ascending=[True, False])
    matches = matches.drop_duplicates(subset=["patient_id"], keep="first")

    return matches


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    source_a, source_b = load_and_prepare()
    matches = build_candidate_pairs(source_a, source_b)

    if matches.empty:
        print("No candidate matches found.")
        return

    matches.to_csv(PROCESSED_DIR / "match_candidates.csv", index=False)

    print(matches.head(15).to_string(index=False))
    print("\nMatch status counts:")
    print(matches["match_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
