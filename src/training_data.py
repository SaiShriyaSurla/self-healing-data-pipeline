from __future__ import annotations

import random
from pathlib import Path

import pandas as pd
from rapidfuzz import fuzz

from features import load_and_prepare


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

random.seed(42)


def similarity_score(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return fuzz.ratio(left, right) / 100.0


def extract_numeric_id(value: str) -> int | None:
    if not isinstance(value, str):
        return None
    digits = "".join(ch for ch in value if ch.isdigit())
    return int(digits) if digits else None


def build_feature_row(a_row: pd.Series, b_row: pd.Series, label: int) -> dict[str, float | int | str]:
    return {
        "source_a_id": a_row["patient_id"],
        "source_b_id": b_row["member_id"],
        "name_score": round(similarity_score(a_row["normalized_name"], b_row["normalized_name"]), 3),
        "address_score": round(similarity_score(a_row["normalized_address"], b_row["normalized_address"]), 3),
        "email_match": int(
            bool(a_row["normalized_email"])
            and bool(b_row["normalized_email"])
            and a_row["normalized_email"] == b_row["normalized_email"]
        ),
        "phone_match": int(
            bool(a_row["normalized_phone"])
            and bool(b_row["normalized_phone"])
            and a_row["normalized_phone"] == b_row["normalized_phone"]
        ),
        "dob_match": int(
            bool(a_row["normalized_dob"])
            and bool(b_row["normalized_dob"])
            and a_row["normalized_dob"] == b_row["normalized_dob"]
        ),
        "label": label,
    }


def main() -> None:
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    source_a, source_b = load_and_prepare()

    source_a = source_a[~source_a["patient_id"].str.contains("LEGACY", na=False)].copy()
    source_b = source_b.copy()

    source_b["numeric_id"] = source_b["member_id"].apply(extract_numeric_id)
    source_a["numeric_id"] = source_a["patient_id"].apply(extract_numeric_id)

    source_b_by_id = {
        row["numeric_id"]: row
        for _, row in source_b.iterrows()
        if pd.notna(row["numeric_id"])
    }

    positive_rows = []
    for _, a_row in source_a.iterrows():
        numeric_id = a_row["numeric_id"]
        b_row = source_b_by_id.get(numeric_id)
        if b_row is not None:
            positive_rows.append(build_feature_row(a_row, b_row, label=1))

    negative_rows = []
    b_records = list(source_b.to_dict(orient="records"))

    for _, a_row in source_a.iterrows():
        a_numeric_id = a_row["numeric_id"]

        candidate = None
        for _ in range(20):
            sampled = random.choice(b_records)
            if sampled["numeric_id"] != a_numeric_id:
                candidate = sampled
                break

        if candidate is not None:
            negative_rows.append(build_feature_row(a_row, pd.Series(candidate), label=0))

    dataset = pd.DataFrame(positive_rows + negative_rows)
    dataset = dataset.sample(frac=1, random_state=42).reset_index(drop=True)

    split_index = int(len(dataset) * 0.8)
    train_df = dataset.iloc[:split_index].copy()
    validation_df = dataset.iloc[split_index:].copy()

    batch_input = validation_df.drop(columns=["label"]).copy()

    train_df.to_csv(PROCESSED_DIR / "train.csv", index=False)
    validation_df.to_csv(PROCESSED_DIR / "validation.csv", index=False)
    batch_input.to_csv(PROCESSED_DIR / "batch_input.csv", index=False)

    print("Train rows:", len(train_df))
    print("Validation rows:", len(validation_df))
    print("Batch input rows:", len(batch_input))
    print("\nLabel distribution:")
    print(dataset["label"].value_counts().to_string())


if __name__ == "__main__":
    main()
