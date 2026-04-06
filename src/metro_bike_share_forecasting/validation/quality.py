from __future__ import annotations

from typing import Any

import pandas as pd

from metro_bike_share_forecasting.ingestion.csv_loader import SourceFileProfile


def build_raw_ingestion_log(
    profiles: list[SourceFileProfile],
    cleaned_data: pd.DataFrame,
    pipeline_run_id: str,
) -> pd.DataFrame:
    if cleaned_data.empty:
        cleaned_counts = {}
        min_ts = {}
        max_ts = {}
    else:
        cleaned_counts = cleaned_data.groupby("source_file")["trip_id"].count().to_dict()
        min_ts = cleaned_data.groupby("source_file")["start_ts_utc"].min().astype(str).to_dict()
        max_ts = cleaned_data.groupby("source_file")["start_ts_utc"].max().astype(str).to_dict()

    rows: list[dict[str, Any]] = []
    for profile in profiles:
        rows.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "source_file": profile.source_file,
                "ingestion_timestamp": pd.Timestamp.utcnow(),
                "record_count_raw": profile.row_count_raw,
                "record_count_cleaned": int(cleaned_counts.get(profile.source_file, 0)),
                "min_event_timestamp": min_ts.get(profile.source_file),
                "max_event_timestamp": max_ts.get(profile.source_file),
                "duplicates_removed": None,
                "schema_drift_detected": profile.schema_drift_detected,
                "schema_columns": profile.column_names,
            }
        )
    return pd.DataFrame(rows)


def build_data_quality_monitoring(
    pipeline_run_id: str,
    quality_summary: dict[str, Any],
    schema_profiles: list[SourceFileProfile],
) -> pd.DataFrame:
    rows = [
        {
            "pipeline_run_id": pipeline_run_id,
            "check_name": "raw_records",
            "check_value": quality_summary["records_raw"],
            "status": "pass",
            "details": {"records_raw": quality_summary["records_raw"]},
        },
        {
            "pipeline_run_id": pipeline_run_id,
            "check_name": "duplicates_removed",
            "check_value": quality_summary["duplicates_removed"],
            "status": "pass",
            "details": {"duplicates_removed": quality_summary["duplicates_removed"]},
        },
        {
            "pipeline_run_id": pipeline_run_id,
            "check_name": "schema_drift_detected",
            "check_value": int(any(profile.schema_drift_detected for profile in schema_profiles)),
            "status": "warn" if any(profile.schema_drift_detected for profile in schema_profiles) else "pass",
            "details": {
                "schema_drift_files": [
                    profile.source_file
                    for profile in schema_profiles
                    if profile.schema_drift_detected
                ]
            },
        },
    ]
    for column_name, missing_share in quality_summary["missingness_summary"].items():
        rows.append(
            {
                "pipeline_run_id": pipeline_run_id,
                "check_name": f"missingness_{column_name}",
                "check_value": missing_share,
                "status": "warn" if missing_share > 0.05 else "pass",
                "details": {"column_name": column_name, "missing_share": missing_share},
            }
        )
    return pd.DataFrame(rows)

