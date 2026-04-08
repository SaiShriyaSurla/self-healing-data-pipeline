from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import streamlit as st


BASE_DIR = Path(__file__).resolve().parents[1]
PROCESSED_DIR = BASE_DIR / "data" / "processed"

REPORT_PATH = PROCESSED_DIR / "reconciliation_report.json"
MATCHES_PATH = PROCESSED_DIR / "match_candidates.csv"
UNIFIED_PATH = PROCESSED_DIR / "unified_patients.csv"
REVIEW_PATH = PROCESSED_DIR / "manual_review.csv"


st.set_page_config(
    page_title="Self-Healing Data Pipeline",
    page_icon="🧩",
    layout="wide",
)


@st.cache_data
def load_report() -> dict[str, object]:
    with REPORT_PATH.open(encoding="utf-8") as file_handle:
        return json.load(file_handle)


@st.cache_data
def load_dataframe(path: Path) -> pd.DataFrame:
    return pd.read_csv(path)


def main() -> None:
    st.title("Self-Healing Data Pipeline with AI Reconciliation")
    st.caption(
        "Detect schema drift, duplicates, broken joins, cross-source record matches, "
        "and unified patient records."
    )

    report = load_report()
    matches = load_dataframe(MATCHES_PATH)
    unified = load_dataframe(UNIFIED_PATH)
    review = load_dataframe(REVIEW_PATH)

    summary = report["summary"]
    quality = report["data_quality"]

    metric_columns = st.columns(4)
    metric_columns[0].metric("Source A Rows", summary["source_a_rows"])
    metric_columns[1].metric("Source B Rows", summary["source_b_rows"])
    metric_columns[2].metric("Auto Matches", summary["auto_matched_records"])
    metric_columns[3].metric("Review Queue", summary["manual_review_queue"])

    metric_columns = st.columns(4)
    metric_columns[0].metric("Unified Patients", summary["unified_patients_created"])
    metric_columns[1].metric("Appointments", summary["appointments_rows"])
    metric_columns[2].metric("Missing Emails", quality["source_b_missing_email"])
    metric_columns[3].metric("Orphan Appointment Refs", quality["appointments_orphan_patient_ref"])

    st.info(
        "This dashboard highlights where fragmented patient data causes reconciliation risk: "
        "missing contact fields, duplicate identities, and broken appointment references. "
        "High-confidence matches are merged automatically, while uncertain matches are routed "
        "to a manual review queue."
    )

    summary_tab, matches_tab, unified_tab, review_tab = st.tabs(
        ["Summary", "Match Candidates", "Unified Records", "Manual Review"]
    )

    with summary_tab:
        st.subheader("Data Quality Findings")
        quality_df = pd.DataFrame(
            [
                {"issue": "Source B missing email", "count": quality["source_b_missing_email"]},
                {"issue": "Source B missing postal code", "count": quality["source_b_missing_postal_code"]},
                {"issue": "Appointments blank patient ref", "count": quality["appointments_blank_patient_ref"]},
                {"issue": "Appointments orphan patient ref", "count": quality["appointments_orphan_patient_ref"]},
                {"issue": "Source A duplicate rows", "count": quality["source_a_duplicate_rows"]},
                {"issue": "Source B duplicate rows", "count": quality["source_b_duplicate_rows"]},
            ]
        )
        st.dataframe(quality_df, width="stretch", hide_index=True)

        st.subheader("Sample Unified Records")
        st.dataframe(unified.head(20), width="stretch")

    with matches_tab:
        st.subheader("Match Candidates")
        match_filter = st.selectbox(
            "Filter by match status",
            options=["all", "auto_match", "review", "no_match"],
            index=1,
        )
        filtered_matches = matches if match_filter == "all" else matches[matches["match_status"] == match_filter]
        st.dataframe(filtered_matches.head(100), width="stretch")

    with unified_tab:
        st.subheader("Unified Patient Index")
        st.dataframe(unified.head(100), width="stretch")

    with review_tab:
        st.subheader("Manual Review Queue")
        st.caption("These records were close matches, but not strong enough to merge automatically.")
        review_only = review[review["match_status"] == "review"] if "match_status" in review.columns else review
        st.dataframe(review_only.head(100), width="stretch")


if __name__ == "__main__":
    main()
