from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict

import pandas as pd

from metro_bike_share_forecasting.evaluation.scoring import score_prediction_frame
from metro_bike_share_forecasting.utils.time import infer_season_length


@dataclass
class BacktestConfig:
    frequency: str
    horizon: int
    initial_window: int
    step: int
    max_folds: int


def generate_rolling_folds(frame: pd.DataFrame, config: BacktestConfig) -> list[tuple[int, pd.DataFrame, pd.DataFrame]]:
    ordered = frame.sort_values("bucket_start").reset_index(drop=True)
    total_rows = len(ordered)
    if total_rows < config.initial_window + config.horizon:
        return []

    candidate_train_ends = list(range(config.initial_window, total_rows - config.horizon + 1, config.step))
    selected_train_ends = candidate_train_ends[-config.max_folds :]

    folds: list[tuple[int, pd.DataFrame, pd.DataFrame]] = []
    for fold_id, train_end in enumerate(selected_train_ends, start=1):
        train = ordered.iloc[:train_end].copy()
        test = ordered.iloc[train_end : train_end + config.horizon].copy()
        folds.append((fold_id, train, test))
    return folds


def describe_rolling_folds(frame: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    frame = frame.sort_values("bucket_start").reset_index(drop=True)
    rows: list[dict[str, object]] = []
    for fold_id, train, test in generate_rolling_folds(frame, config):
        rows.append(
            {
                "fold_id": fold_id,
                "frequency": config.frequency,
                "initial_window": config.initial_window,
                "horizon": config.horizon,
                "step": config.step,
                "training_window_start": train["bucket_start"].min(),
                "training_window_end": train["bucket_start"].max(),
                "test_window_start": test["bucket_start"].min(),
                "test_window_end": test["bucket_start"].max(),
                "train_rows": len(train),
                "test_rows": len(test),
            }
        )
    return pd.DataFrame(rows)


def _forecast_holdout(
    history: pd.DataFrame,
    holdout: pd.DataFrame,
    model_name: str,
    factory: Callable[[], object],
    frequency: str,
    fold_id: int | None,
    window_role: str,
) -> pd.DataFrame:
    model = factory()
    model.fit(history)
    forecast = model.forecast(history, horizon=len(holdout)).reset_index(drop=True)

    joined = forecast.copy()
    joined["actual"] = holdout["trip_count"].to_numpy()
    joined["frequency"] = frequency
    joined["window_role"] = window_role
    joined["fold_id"] = fold_id
    joined["horizon_step"] = range(1, len(joined) + 1)
    joined["evaluation_regime"] = holdout["pandemic_phase"].to_numpy() if "pandemic_phase" in holdout.columns else "unknown"
    joined["training_window_start"] = history["bucket_start"].min()
    joined["training_window_end"] = history["bucket_start"].max()
    joined["holdout_window_start"] = holdout["bucket_start"].min()
    joined["holdout_window_end"] = holdout["bucket_start"].max()
    joined["model_name"] = model_name
    return joined


def run_backtest(
    frame: pd.DataFrame,
    model_factories: Dict[str, Callable[[], object]],
    config: BacktestConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    frame = frame.sort_values("bucket_start").reset_index(drop=True)
    prediction_rows: list[pd.DataFrame] = []
    metric_rows: list[pd.DataFrame] = []
    season_length = infer_season_length(config.frequency)

    for fold_id, train, test in generate_rolling_folds(frame, config):
        for model_name, factory in model_factories.items():
            joined = _forecast_holdout(
                history=train,
                holdout=test,
                model_name=model_name,
                factory=factory,
                frequency=config.frequency,
                fold_id=fold_id,
                window_role="rolling_backtest",
            )
            prediction_rows.append(joined)
            metric_rows.append(
                score_prediction_frame(
                    joined,
                    training_series=train["trip_count"],
                    season_length=season_length,
                    metadata={
                        "model_name": model_name,
                        "fold_id": fold_id,
                        "frequency": config.frequency,
                        "window_role": "rolling_backtest",
                        "training_window_start": train["bucket_start"].min(),
                        "training_window_end": train["bucket_start"].max(),
                        "holdout_window_start": test["bucket_start"].min(),
                        "holdout_window_end": test["bucket_start"].max(),
                    },
                )
            )

    predictions = pd.concat(prediction_rows, ignore_index=True) if prediction_rows else pd.DataFrame()
    metrics = pd.concat(metric_rows, ignore_index=True) if metric_rows else pd.DataFrame()
    return predictions, metrics


def evaluate_holdout(
    train_frame: pd.DataFrame,
    holdout_frame: pd.DataFrame,
    model_factories: Dict[str, Callable[[], object]],
    frequency: str,
    window_role: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    ordered_train = train_frame.sort_values("bucket_start").reset_index(drop=True)
    ordered_holdout = holdout_frame.sort_values("bucket_start").reset_index(drop=True)
    season_length = infer_season_length(frequency)

    prediction_rows: list[pd.DataFrame] = []
    metric_rows: list[pd.DataFrame] = []
    for model_name, factory in model_factories.items():
        joined = _forecast_holdout(
            history=ordered_train,
            holdout=ordered_holdout,
            model_name=model_name,
            factory=factory,
            frequency=frequency,
            fold_id=None,
            window_role=window_role,
        )
        prediction_rows.append(joined)
        metric_rows.append(
            score_prediction_frame(
                joined,
                training_series=ordered_train["trip_count"],
                season_length=season_length,
                metadata={
                    "model_name": model_name,
                    "fold_id": None,
                    "frequency": frequency,
                    "window_role": window_role,
                    "training_window_start": ordered_train["bucket_start"].min(),
                    "training_window_end": ordered_train["bucket_start"].max(),
                    "holdout_window_start": ordered_holdout["bucket_start"].min(),
                    "holdout_window_end": ordered_holdout["bucket_start"].max(),
                },
            )
        )

    predictions = pd.concat(prediction_rows, ignore_index=True) if prediction_rows else pd.DataFrame()
    metrics = pd.concat(metric_rows, ignore_index=True) if metric_rows else pd.DataFrame()
    return predictions, metrics
