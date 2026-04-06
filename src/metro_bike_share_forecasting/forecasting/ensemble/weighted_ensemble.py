from __future__ import annotations

from typing import Dict

import pandas as pd

from metro_bike_share_forecasting.evaluation.scoring import score_prediction_frame


def compute_inverse_error_weights(backtest_summary: pd.DataFrame, top_n: int = 3) -> dict[str, float]:
    ranked = backtest_summary.sort_values("composite_score").head(top_n).copy()
    ranked["weight"] = 1 / ranked["composite_score"].clip(lower=1e-6)
    ranked["weight"] = ranked["weight"] / ranked["weight"].sum()
    return dict(zip(ranked["model_name"], ranked["weight"]))


def combine_forecasts(forecasts: Dict[str, pd.DataFrame], weights: dict[str, float]) -> pd.DataFrame:
    reference = None
    for model_name, frame in forecasts.items():
        weighted = frame.copy()
        weight = weights.get(model_name, 0.0)
        for column in ["prediction", "lower_50", "upper_50", "lower_80", "upper_80", "lower_95", "upper_95"]:
            weighted[column] = weighted[column] * weight
        if reference is None:
            reference = weighted
        else:
            reference = reference.merge(
                weighted,
                on="target_timestamp",
                suffixes=("", f"_{model_name}"),
            )

    if reference is None:
        return pd.DataFrame()

    ensemble = pd.DataFrame({"target_timestamp": reference["target_timestamp"]})
    for column in ["prediction", "lower_50", "upper_50", "lower_80", "upper_80", "lower_95", "upper_95"]:
        matching = [candidate for candidate in reference.columns if candidate == column or candidate.startswith(f"{column}_")]
        ensemble[column] = reference[matching].sum(axis=1)
    ensemble["model_name"] = "weighted_ensemble"
    return ensemble


def combine_backtest_predictions(
    prediction_frame: pd.DataFrame,
    weights: dict[str, float],
    season_length: int,
    training_series: pd.Series,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if prediction_frame.empty:
        return pd.DataFrame(), pd.DataFrame()

    weighted_frames = []
    for model_name, group in prediction_frame.groupby("model_name"):
        if model_name not in weights:
            continue
        weighted = group.copy()
        for column in ["prediction", "lower_50", "upper_50", "lower_80", "upper_80", "lower_95", "upper_95"]:
            weighted[column] = weighted[column] * weights[model_name]
        weighted_frames.append(weighted)

    if not weighted_frames:
        return pd.DataFrame(), pd.DataFrame()

    combined = pd.concat(weighted_frames, ignore_index=True)
    aggregated = (
        combined.groupby(
            [
                "window_role",
                "fold_id",
                "frequency",
                "horizon_step",
                "target_timestamp",
                "evaluation_regime",
                "actual",
                "training_window_start",
                "training_window_end",
                "holdout_window_start",
                "holdout_window_end",
            ],
            as_index=False,
        )
        .agg(
            prediction=("prediction", "sum"),
            lower_50=("lower_50", "sum"),
            upper_50=("upper_50", "sum"),
            lower_80=("lower_80", "sum"),
            upper_80=("upper_80", "sum"),
            lower_95=("lower_95", "sum"),
            upper_95=("upper_95", "sum"),
        )
    )
    aggregated["model_name"] = "weighted_ensemble"

    metric_frames = []
    grouped = aggregated.groupby(
        [
            "window_role",
            "frequency",
            "fold_id",
            "training_window_start",
            "training_window_end",
            "holdout_window_start",
            "holdout_window_end",
        ],
        dropna=False,
    )
    for keys, group in grouped:
        window_role, frequency, fold_id, train_start, train_end, holdout_start, holdout_end = keys
        metric_frames.append(
            score_prediction_frame(
                group,
                training_series=training_series,
                season_length=season_length,
                metadata={
                    "model_name": "weighted_ensemble",
                    "fold_id": fold_id,
                    "frequency": frequency,
                    "window_role": window_role,
                    "training_window_start": train_start,
                    "training_window_end": train_end,
                    "holdout_window_start": holdout_start,
                    "holdout_window_end": holdout_end,
                },
            )
        )
    metrics = pd.concat(metric_frames, ignore_index=True) if metric_frames else pd.DataFrame()
    return aggregated, metrics
