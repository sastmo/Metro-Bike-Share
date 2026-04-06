from __future__ import annotations

from typing import Dict

import pandas as pd

from metro_bike_share_forecasting.utils.time import FREQUENCY_TO_PANDAS_ALIAS, bucket_end_from_start


def _assign_bucket_start(index: pd.Series, frequency: str) -> pd.Series:
    alias = FREQUENCY_TO_PANDAS_ALIAS[frequency]
    if frequency in {"hourly", "daily"}:
        return index.dt.floor(alias)
    if frequency == "weekly":
        return index.dt.floor("D") - pd.to_timedelta(index.dt.dayofweek, unit="D")
    if frequency == "monthly":
        return index.dt.to_period("M").dt.to_timestamp()
    if frequency == "quarterly":
        return index.dt.to_period("Q").dt.to_timestamp()
    raise ValueError(f"Unsupported frequency: {frequency}")


def _complete_series(frame: pd.DataFrame, frequency: str) -> pd.DataFrame:
    completed_frames: list[pd.DataFrame] = []
    alias = FREQUENCY_TO_PANDAS_ALIAS[frequency]

    for (segment_type, segment_id), group in frame.groupby(["segment_type", "segment_id"], dropna=False):
        if group.empty:
            continue
        start = group["bucket_start"].min()
        end = group["bucket_start"].max()
        full_index = pd.date_range(start=start, end=end, freq=alias)
        completed = (
            group.set_index("bucket_start")
            .reindex(full_index)
            .rename_axis("bucket_start")
            .reset_index()
        )
        completed["segment_type"] = segment_type
        completed["segment_id"] = segment_id
        completed["trip_count"] = completed["trip_count"].fillna(0).astype(int)
        completed["distinct_bikes"] = completed["distinct_bikes"].fillna(0).astype(int)
        completed["avg_duration_minutes"] = completed["avg_duration_minutes"].fillna(0.0)
        completed["missing_period_flag"] = completed["is_observed"].isna()
        completed["is_observed"] = completed["is_observed"].where(completed["is_observed"].notna(), False).astype(bool)
        completed_frames.append(completed)

    if not completed_frames:
        return frame
    return pd.concat(completed_frames, ignore_index=True)


def aggregate_cleaned_trips(cleaned_data: pd.DataFrame, frequency: str) -> pd.DataFrame:
    if cleaned_data.empty:
        return pd.DataFrame()

    base = cleaned_data.copy()
    base["bucket_start"] = _assign_bucket_start(base["start_ts_local"], frequency)

    total_aggregate = (
        base.groupby("bucket_start", as_index=False)
        .agg(
            trip_count=("trip_id", "count"),
            distinct_bikes=("bike_id", "nunique"),
            avg_duration_minutes=("duration_minutes", "mean"),
        )
        .assign(segment_type="system_total", segment_id="all", is_observed=True)
    )

    station_aggregate = (
        base.groupby(["bucket_start", "start_station"], as_index=False)
        .agg(
            trip_count=("trip_id", "count"),
            distinct_bikes=("bike_id", "nunique"),
            avg_duration_minutes=("duration_minutes", "mean"),
        )
        .rename(columns={"start_station": "segment_id"})
        .assign(segment_type="start_station", is_observed=True)
    )

    aggregate = pd.concat([total_aggregate, station_aggregate], ignore_index=True, sort=False)
    aggregate = _complete_series(aggregate, frequency)
    aggregate["bucket_end"] = bucket_end_from_start(aggregate["bucket_start"], frequency)
    aggregate["generated_at"] = pd.Timestamp.utcnow()
    aggregate["segment_id"] = aggregate["segment_id"].astype(str)
    return aggregate.sort_values(["segment_type", "segment_id", "bucket_start"]).reset_index(drop=True)


def build_multigranularity_aggregates(cleaned_data: pd.DataFrame, frequencies: tuple[str, ...] | None = None) -> Dict[str, pd.DataFrame]:
    frequencies = frequencies or ("hourly", "daily", "weekly", "monthly", "quarterly")
    return {
        frequency: aggregate_cleaned_trips(cleaned_data, frequency)
        for frequency in frequencies
    }
