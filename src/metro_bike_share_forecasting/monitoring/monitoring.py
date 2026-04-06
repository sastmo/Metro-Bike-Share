from __future__ import annotations

from typing import Any

import pandas as pd


def build_drift_monitoring(
    aggregate_frame: pd.DataFrame,
    pipeline_run_id: str,
    frequency: str,
) -> pd.DataFrame:
    if aggregate_frame.empty:
        return pd.DataFrame()

    system_total = aggregate_frame.loc[aggregate_frame["segment_type"] == "system_total"].copy()
    if system_total.empty:
        return pd.DataFrame()

    system_total = system_total.sort_values("bucket_start")
    recent = system_total.tail(max(4, len(system_total) // 10))
    historical = system_total.iloc[:-len(recent)] if len(system_total) > len(recent) else system_total

    rows: list[dict[str, Any]] = [
        {
            "pipeline_run_id": pipeline_run_id,
            "frequency": frequency,
            "metric_name": "recent_mean_vs_history_mean",
            "metric_value": float(recent["trip_count"].mean() - historical["trip_count"].mean()),
            "details": {
                "recent_mean": float(recent["trip_count"].mean()),
                "historical_mean": float(historical["trip_count"].mean()),
            },
        },
        {
            "pipeline_run_id": pipeline_run_id,
            "frequency": frequency,
            "metric_name": "recent_zero_share",
            "metric_value": float((recent["trip_count"] == 0).mean()),
            "details": {"recent_rows": int(len(recent))},
        },
    ]
    return pd.DataFrame(rows)

