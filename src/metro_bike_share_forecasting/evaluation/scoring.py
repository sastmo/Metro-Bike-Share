from __future__ import annotations

from typing import Any

import pandas as pd

from metro_bike_share_forecasting.evaluation.metrics import (
    bias,
    interval_coverage,
    interval_width,
    mae,
    mape,
    mase,
    pinball_loss,
    rmse,
    smape,
)


INTERVAL_LEVELS = (50, 80, 95)


def assign_horizon_buckets(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty or "horizon_step" not in frame.columns:
        return frame

    enriched = frame.copy()
    max_horizon = int(enriched["horizon_step"].max())
    if max_horizon <= 3:
        enriched["horizon_bucket"] = "full_horizon"
        return enriched

    short_end = max(1, max_horizon // 3)
    medium_end = max(short_end + 1, (2 * max_horizon) // 3)

    def label(step: int) -> str:
        if step <= short_end:
            return "short"
        if step <= medium_end:
            return "medium"
        return "long"

    enriched["horizon_bucket"] = enriched["horizon_step"].map(label)
    return enriched


def _score_subset(
    subset: pd.DataFrame,
    training_series: pd.Series,
    season_length: int,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    actual = subset["actual"]
    predicted = subset["prediction"]
    row = {
        **metadata,
        "holdout_rows": len(subset),
        "mae": mae(actual, predicted),
        "rmse": rmse(actual, predicted),
        "mape": mape(actual, predicted),
        "smape": smape(actual, predicted),
        "mase": mase(actual, predicted, training_series, season_length),
        "bias": bias(actual, predicted),
    }
    for level in INTERVAL_LEVELS:
        row[f"pinball_{level}"] = pinball_loss(
            actual,
            subset[f"lower_{level}"],
            subset[f"upper_{level}"],
            level / 100,
        )
        row[f"coverage_{level}"] = interval_coverage(actual, subset[f"lower_{level}"], subset[f"upper_{level}"])
        row[f"width_{level}"] = interval_width(subset[f"lower_{level}"], subset[f"upper_{level}"])
    return row


def score_prediction_frame(
    prediction_frame: pd.DataFrame,
    training_series: pd.Series,
    season_length: int,
    metadata: dict[str, Any],
) -> pd.DataFrame:
    if prediction_frame.empty:
        return pd.DataFrame()

    scored = assign_horizon_buckets(prediction_frame)
    rows = [
        _score_subset(
            scored,
            training_series=training_series,
            season_length=season_length,
            metadata={
                **metadata,
                "metric_scope": "overall",
                "horizon_bucket": "all",
                "evaluation_regime": "all",
            },
        )
    ]

    if "horizon_bucket" in scored.columns:
        for horizon_bucket, subset in scored.groupby("horizon_bucket"):
            rows.append(
                _score_subset(
                    subset,
                    training_series=training_series,
                    season_length=season_length,
                    metadata={
                        **metadata,
                        "metric_scope": "horizon_bucket",
                        "horizon_bucket": horizon_bucket,
                        "evaluation_regime": "all",
                    },
                )
            )

    if "evaluation_regime" in scored.columns and scored["evaluation_regime"].nunique(dropna=False) > 1:
        for regime, subset in scored.groupby("evaluation_regime"):
            rows.append(
                _score_subset(
                    subset,
                    training_series=training_series,
                    season_length=season_length,
                    metadata={
                        **metadata,
                        "metric_scope": "regime",
                        "horizon_bucket": "all",
                        "evaluation_regime": regime,
                    },
                )
            )

    return pd.DataFrame(rows)
