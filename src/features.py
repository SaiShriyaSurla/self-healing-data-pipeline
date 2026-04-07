from __future__ import annotations

import re
from pathlib import Path

import pandas as pd


BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"

SOURCE_A_PATH = RAW_DIR / "patients_source_a.csv"
SOURCE_B_PATH = RAW_DIR / "patients_source_b.csv"


def normalize_text(value: object) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = re.sub(r"\s+", " ", value)
    return value


def normalize_phone(value: object) -> str:
    if pd.isna(value):
        return ""
    return re.sub(r"\D", "", str(value))


def normalize_email(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().lower()


def normalize_address(value: object) -> str:
    if pd.isna(value):
        return ""
    value = str(value).strip().lower()
    value = value.replace("avenida", "av")
    value = value.replace("rua", "r")
    value = value.replace("travessa", "tv")
    value = re.sub(r"\s+", " ", value)
    return value


def prepare_source_a(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["full_name"] = (
        out["first_name"].fillna("").astype(str).str.strip()
        + " "
        + out["last_name"].fillna("").astype(str).str.strip()
    ).str.strip()

    out["normalized_name"] = out["full_name"].apply(normalize_text)
    out["normalized_email"] = out["email"].apply(normalize_email)
    out["normalized_phone"] = out["phone"].apply(normalize_phone)
    out["normalized_address"] = out["address"].apply(normalize_address)
    out["normalized_dob"] = out["date_of_birth"].fillna("").astype(str)

    out["source_system"] = "source_a"
    out["source_record_id"] = out["patient_id"]

    return out


def prepare_source_b(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    out["normalized_name"] = out["full_name"].apply(normalize_text)
    out["normalized_email"] = out["email_address"].apply(normalize_email)
    out["normalized_phone"] = out["phone_number"].apply(normalize_phone)
    out["normalized_address"] = out["street"].apply(normalize_address)
    out["normalized_dob"] = out["dob"].fillna("").astype(str)

    out["source_system"] = "source_b"
    out["source_record_id"] = out["member_id"]

    return out


def load_and_prepare() -> tuple[pd.DataFrame, pd.DataFrame]:
    source_a = pd.read_csv(SOURCE_A_PATH)
    source_b = pd.read_csv(SOURCE_B_PATH)

    return prepare_source_a(source_a), prepare_source_b(source_b)


def main() -> None:
    source_a_prepared, source_b_prepared = load_and_prepare()

    print("SOURCE A")
    print(
        source_a_prepared[
            [
                "patient_id",
                "full_name",
                "normalized_name",
                "normalized_email",
                "normalized_phone",
                "normalized_address",
                "normalized_dob",
            ]
        ].head(5).to_string(index=False)
    )

    print("\nSOURCE B")
    print(
        source_b_prepared[
            [
                "member_id",
                "full_name",
                "normalized_name",
                "normalized_email",
                "normalized_phone",
                "normalized_address",
                "normalized_dob",
            ]
        ].head(5).to_string(index=False)
    )


if __name__ == "__main__":
    main()
