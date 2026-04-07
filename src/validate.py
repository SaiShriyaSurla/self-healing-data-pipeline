from __future__ import annotations

import json
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

SOURCE_A_PATH = RAW_DIR / "patients_source_a.csv"
SOURCE_B_PATH = RAW_DIR / "patients_source_b.csv"
APPOINTMENTS_PATH = RAW_DIR / "appointments.csv"


EXPECTED_SOURCE_A_COLUMNS = [
    "patient_id",
    "first_name",
    "last_name",
    "date_of_birth",
    "email",
    "phone",
    "address",
    "city",
    "state",
    "zip_code",
]

EXPECTED_SOURCE_B_COLUMNS = [
    "member_id",
    "full_name",
    "dob",
    "email_address",
    "phone_number",
    "street",
    "city_name",
    "state_code",
    "postal_code",
]

EXPECTED_APPOINTMENTS_COLUMNS = [
    "appointment_id",
    "patient_ref",
    "appointment_date",
    "scheduled_date",
    "department",
    "provider",
    "status",
    "no_show_flag",
]


def load_dataframes() -> dict[str, pd.DataFrame]:
    return {
        "patients_source_a": pd.read_csv(SOURCE_A_PATH),
        "patients_source_b": pd.read_csv(SOURCE_B_PATH),
        "appointments": pd.read_csv(APPOINTMENTS_PATH),
    }


def schema_summary(df: pd.DataFrame, expected_columns: list[str]) -> dict[str, list[str]]:
    actual = list(df.columns)
    return {
        "expected": expected_columns,
        "actual": actual,
        "missing_columns": [col for col in expected_columns if col not in actual],
        "unexpected_columns": [col for col in actual if col not in expected_columns],
    }


def null_summary(df: pd.DataFrame) -> dict[str, int]:
    return {
        column: int(count)
        for column, count in df.isna().sum().items()
        if int(count) > 0
    }


def duplicate_summary(df: pd.DataFrame, subset: list[str]) -> dict[str, object]:
    duplicate_mask = df.duplicated(subset=subset, keep=False)
    duplicate_rows = df.loc[duplicate_mask, subset].fillna("").head(10).to_dict(orient="records")
    return {
        "duplicate_row_count": int(duplicate_mask.sum()),
        "sample_duplicate_keys": duplicate_rows,
    }


def broken_join_summary(appointments_df: pd.DataFrame, source_a_df: pd.DataFrame) -> dict[str, object]:
    patient_ids = set(source_a_df["patient_id"].dropna().astype(str))
    refs = appointments_df["patient_ref"].fillna("").astype(str)

    blank_refs = appointments_df.loc[refs.eq(""), ["appointment_id", "patient_ref"]].head(10)
    orphan_refs = appointments_df.loc[
        refs.ne("") & ~refs.isin(patient_ids),
        ["appointment_id", "patient_ref"],
    ].head(10)

    return {
        "blank_patient_ref_count": int(refs.eq("").sum()),
        "orphan_patient_ref_count": int((refs.ne("") & ~refs.isin(patient_ids)).sum()),
        "sample_blank_refs": blank_refs.to_dict(orient="records"),
        "sample_orphan_refs": orphan_refs.to_dict(orient="records"),
    }


def validate_sources() -> dict[str, object]:
    dfs = load_dataframes()
    source_a_df = dfs["patients_source_a"]
    source_b_df = dfs["patients_source_b"]
    appointments_df = dfs["appointments"]

    report = {
        "row_counts": {
            "patients_source_a": int(len(source_a_df)),
            "patients_source_b": int(len(source_b_df)),
            "appointments": int(len(appointments_df)),
        },
        "schema": {
            "patients_source_a": schema_summary(source_a_df, EXPECTED_SOURCE_A_COLUMNS),
            "patients_source_b": schema_summary(source_b_df, EXPECTED_SOURCE_B_COLUMNS),
            "appointments": schema_summary(appointments_df, EXPECTED_APPOINTMENTS_COLUMNS),
        },
        "nulls": {
            "patients_source_a": null_summary(source_a_df),
            "patients_source_b": null_summary(source_b_df),
            "appointments": null_summary(appointments_df),
        },
        "duplicates": {
            "patients_source_a": duplicate_summary(
                source_a_df,
                ["first_name", "last_name", "date_of_birth", "address"],
            ),
            "patients_source_b": duplicate_summary(
                source_b_df,
                ["full_name", "dob", "street"],
            ),
        },
        "broken_joins": broken_join_summary(appointments_df, source_a_df),
    }

    return report


def main() -> None:
    report = validate_sources()
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
