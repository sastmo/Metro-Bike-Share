from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import pandas as pd

from metro_bike_share_forecasting.cleaning.legacy_rules import (
    build_missingness_summary,
    describe_legacy_reuse,
    normalize_column_name,
    normalize_text_value,
    numeric_cast,
    parse_datetime_series,
)
from metro_bike_share_forecasting.config.settings import Settings


REQUIRED_TRIP_COLUMNS = [
    "trip_id",
    "duration",
    "start_time",
    "end_time",
    "start_station",
    "start_lat",
    "start_lon",
    "end_station",
    "end_lat",
    "end_lon",
    "bike_id",
    "plan_duration",
    "trip_route_category",
    "passholder_type",
    "bike_type",
]


@dataclass
class CleaningResult:
    cleaned_data: pd.DataFrame
    quality_summary: dict[str, Any]
    legacy_reuse: dict[str, Any]


def _ensure_required_columns(frame: pd.DataFrame) -> pd.DataFrame:
    renamed = frame.rename(columns={column: normalize_column_name(column) for column in frame.columns})
    for column in REQUIRED_TRIP_COLUMNS:
        if column not in renamed.columns:
            renamed[column] = pd.NA
    return renamed


def clean_trip_data(raw_data: pd.DataFrame, settings: Settings) -> CleaningResult:
    frame = _ensure_required_columns(raw_data.copy())
    if frame.empty:
        return CleaningResult(
            cleaned_data=pd.DataFrame(),
            quality_summary={
                "records_raw": 0,
                "records_after_dedup": 0,
                "records_cleaned": 0,
                "duplicates_removed": 0,
                "min_event_timestamp_utc": None,
                "max_event_timestamp_utc": None,
                "missingness_summary": {},
            },
            legacy_reuse=describe_legacy_reuse().as_dict(),
        )

    for column in REQUIRED_TRIP_COLUMNS:
        frame[f"raw_{column}"] = frame[column]

    frame["trip_id"] = numeric_cast(frame["trip_id"], target="int")
    frame["duration_minutes"] = numeric_cast(frame["duration"])
    frame["start_station"] = numeric_cast(frame["start_station"], target="int")
    frame["end_station"] = numeric_cast(frame["end_station"], target="int")
    frame["bike_id"] = numeric_cast(frame["bike_id"], target="int")
    frame["plan_duration"] = numeric_cast(frame["plan_duration"], target="int")
    frame["start_lat"] = numeric_cast(frame["start_lat"])
    frame["start_lon"] = numeric_cast(frame["start_lon"])
    frame["end_lat"] = numeric_cast(frame["end_lat"])
    frame["end_lon"] = numeric_cast(frame["end_lon"])

    frame["start_ts_local"] = parse_datetime_series(frame["start_time"])
    frame["end_ts_local"] = parse_datetime_series(frame["end_time"])

    localized_start = frame["start_ts_local"].dt.tz_localize(settings.raw_timezone, ambiguous="NaT", nonexistent="shift_forward")
    localized_end = frame["end_ts_local"].dt.tz_localize(settings.raw_timezone, ambiguous="NaT", nonexistent="shift_forward")
    frame["start_ts_utc"] = localized_start.dt.tz_convert("UTC")
    frame["end_ts_utc"] = localized_end.dt.tz_convert("UTC")

    frame["trip_route_category"] = frame["trip_route_category"].map(normalize_text_value)
    frame["passholder_type"] = frame["passholder_type"].map(normalize_text_value)
    frame["bike_type"] = frame["bike_type"].map(normalize_text_value)

    invalid_trip_id = frame["trip_id"].isna()
    invalid_timestamps = frame["start_ts_utc"].isna() | frame["end_ts_utc"].isna()
    invalid_order = frame["end_ts_utc"] < frame["start_ts_utc"]
    testing_rows = frame["passholder_type"].eq("Testing")
    test_plan_rows = frame["plan_duration"].eq(999)
    invalid_geo = (
        frame["start_lat"].isna()
        | frame["start_lon"].isna()
        | frame["end_lat"].isna()
        | frame["end_lon"].isna()
        | frame["start_lat"].eq(0)
        | frame["start_lon"].eq(0)
        | frame["end_lat"].eq(0)
        | frame["end_lon"].eq(0)
    )

    frame["derived_duration_minutes"] = (frame["end_ts_utc"] - frame["start_ts_utc"]).dt.total_seconds() / 60.0
    frame["duration_minutes"] = frame["duration_minutes"].fillna(frame["derived_duration_minutes"])
    invalid_duration = frame["duration_minutes"].isna() | (frame["duration_minutes"] <= 0)

    before_dedup = len(frame)
    frame = frame.sort_values(["trip_id", "source_file", "source_row_number"], kind="stable")
    frame = frame.drop_duplicates(subset=["trip_id"], keep="first")
    duplicates_removed = before_dedup - len(frame)

    frame = frame.loc[~(invalid_trip_id | invalid_timestamps | invalid_order | testing_rows | test_plan_rows | invalid_geo | invalid_duration)].copy()
    frame["duration_minutes"] = frame["duration_minutes"].astype(float)
    frame["trip_id"] = frame["trip_id"].astype("int64")
    frame["start_station"] = frame["start_station"].astype("Int64")
    frame["end_station"] = frame["end_station"].astype("Int64")
    frame["bike_id"] = frame["bike_id"].astype("Int64")
    frame["plan_duration"] = frame["plan_duration"].astype("Int64")
    frame["event_date_local"] = frame["start_ts_local"].dt.date.astype(str)
    frame["event_hour_local"] = frame["start_ts_local"].dt.hour.astype("Int64")
    frame["event_month_local"] = frame["start_ts_local"].dt.month.astype("Int64")
    frame["event_year_local"] = frame["start_ts_local"].dt.year.astype("Int64")

    quality_summary = {
        "records_raw": int(len(raw_data)),
        "records_after_dedup": int(before_dedup - duplicates_removed),
        "records_cleaned": int(len(frame)),
        "duplicates_removed": int(duplicates_removed),
        "min_event_timestamp_utc": frame["start_ts_utc"].min().isoformat() if not frame.empty else None,
        "max_event_timestamp_utc": frame["start_ts_utc"].max().isoformat() if not frame.empty else None,
        "missingness_summary": build_missingness_summary(frame, ["start_station", "end_station", "bike_id", "plan_duration", "bike_type"]),
    }

    return CleaningResult(
        cleaned_data=frame,
        quality_summary=quality_summary,
        legacy_reuse=describe_legacy_reuse().as_dict(),
    )
