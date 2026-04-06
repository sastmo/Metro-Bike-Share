from __future__ import annotations

import pandas as pd


def summarize_backtests(metric_frame: pd.DataFrame) -> pd.DataFrame:
    if metric_frame.empty:
        return pd.DataFrame()

    scoped = metric_frame.copy()
    if "metric_scope" in scoped.columns:
        scoped = scoped.loc[scoped["metric_scope"] == "overall"].copy()
    if scoped.empty:
        return pd.DataFrame()
    for column, default in (
        ("pinball_50", 0.0),
        ("pinball_95", 0.0),
        ("coverage_50", 0.50),
        ("coverage_95", 0.95),
        ("width_50", 0.0),
        ("width_95", 0.0),
    ):
        if column not in scoped.columns:
            scoped[column] = default

    grouped = (
        scoped.groupby(["model_name", "frequency"], as_index=False)
        .agg(
            mae=("mae", "mean"),
            rmse=("rmse", "mean"),
            smape=("smape", "mean"),
            mase=("mase", "mean"),
            pinball_50=("pinball_50", "mean"),
            pinball_80=("pinball_80", "mean"),
            pinball_95=("pinball_95", "mean"),
            coverage_50=("coverage_50", "mean"),
            coverage_80=("coverage_80", "mean"),
            coverage_95=("coverage_95", "mean"),
            width_50=("width_50", "mean"),
            width_80=("width_80", "mean"),
            width_95=("width_95", "mean"),
            bias=("bias", "mean"),
        )
    )
    grouped["coverage_penalty"] = (
        (grouped["coverage_50"] - 0.50).abs()
        + (grouped["coverage_80"] - 0.80).abs()
        + (grouped["coverage_95"] - 0.95).abs()
    ) / 3
    grouped["bias_penalty"] = grouped["bias"].abs() / grouped["mae"].clip(lower=1e-6)

    ranked_frames = []
    score_columns = [
        "mae",
        "rmse",
        "smape",
        "mase",
        "pinball_50",
        "pinball_80",
        "pinball_95",
        "coverage_penalty",
        "bias_penalty",
    ]
    for frequency, frequency_group in grouped.groupby("frequency", as_index=False):
        ranked = frequency_group.copy()
        rank_columns = []
        for column in score_columns:
            rank_column = f"rank_{column}"
            ranked[rank_column] = ranked[column].rank(method="average", ascending=True, pct=True)
            rank_columns.append(rank_column)
        ranked["composite_score"] = ranked[rank_columns].mean(axis=1)
        ranked_frames.append(ranked)

    grouped = pd.concat(ranked_frames, ignore_index=True)
    return grouped.sort_values(["frequency", "composite_score"]).reset_index(drop=True)


def select_champion_model(summary_frame: pd.DataFrame) -> pd.DataFrame:
    champions = (
        summary_frame.sort_values(["frequency", "composite_score"])
        .groupby("frequency", as_index=False)
        .head(1)
        .copy()
    )
    champions["selection_reason"] = champions.apply(
        lambda row: (
            f"Selected {row['model_name']} for {row['frequency']} because it achieved the lowest composite score "
            f"({row['composite_score']:.4f}) across rolling-origin backtests."
        ),
        axis=1,
    )
    return champions.reset_index(drop=True)
