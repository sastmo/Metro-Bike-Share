from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from metro_bike_share_forecasting.config.settings import get_settings
from metro_bike_share_forecasting.reporting import build_dashboard_context


PLOT_EXPLANATIONS = {
    "series.png": "Observed demand over time with pandemic regime boundaries overlaid.",
    "acf.png": "Autocorrelation plot showing how strongly current demand is related to past lags.",
    "pacf.png": "Partial autocorrelation plot showing which lags add fresh signal after earlier lags are accounted for.",
    "stl.png": "STL decomposition separating trend, seasonal structure, and residual noise.",
    "periodogram.png": "Spectral view of repeating cycles. Large peaks suggest strong seasonal periods worth modeling.",
}


def _run_pipeline(frequencies: list[str], max_backtest_folds: int, persist_to_postgres: bool) -> dict[str, Any]:
    settings = get_settings()
    env = os.environ.copy()
    env["PYTHONPATH"] = str(settings.project_root / "src")
    env["FREQUENCIES"] = ",".join(frequencies)
    env["MAX_BACKTEST_FOLDS"] = str(max_backtest_folds)
    if not persist_to_postgres:
        env["POSTGRES_URL"] = ""

    command = [sys.executable, "-m", "metro_bike_share_forecasting.cli", "run-full-pipeline"]
    result = subprocess.run(
        command,
        cwd=settings.project_root,
        env=env,
        capture_output=True,
        text=True,
    )
    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "command": " ".join(command),
    }


def _apply_green_theme(st) -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top left, rgba(24, 128, 84, 0.10), transparent 24%),
                linear-gradient(180deg, #f5fbf7 0%, #ffffff 22%);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #f0f8f2 0%, #f9fcfa 100%);
        }
        h1, h2, h3 {
            color: #0f5132;
        }
        div[data-testid="stMetric"] {
            background: #f4fbf6;
            border: 1px solid #d5eadb;
            border-radius: 14px;
            padding: 0.75rem 1rem;
        }
        div.stButton > button {
            background: #188054;
            color: white;
            border: 0;
            border-radius: 10px;
        }
        div.stButton > button:hover {
            background: #13663f;
            color: white;
        }
        [data-baseweb="tab-list"] button[aria-selected="true"] {
            color: #188054 !important;
            border-bottom-color: #188054 !important;
        }
        .story-card {
            background: #f4fbf6;
            border: 1px solid #d5eadb;
            border-radius: 16px;
            padding: 1rem 1.1rem;
            height: 100%;
        }
        .story-card-title {
            color: #0f5132;
            font-weight: 700;
            margin-bottom: 0.4rem;
        }
        .story-card-body {
            color: #355244;
            font-size: 0.95rem;
            line-height: 1.45;
        }
        .section-note {
            background: #eef8f1;
            border-left: 4px solid #188054;
            border-radius: 10px;
            padding: 0.85rem 1rem;
            margin: 0.4rem 0 1rem 0;
            color: #244434;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def _artifact_frame(items: list[dict[str, Any]]) -> pd.DataFrame:
    if not items:
        return pd.DataFrame(columns=["name", "relative_path", "size_mb", "modified_at"])
    frame = pd.DataFrame(items)
    frame["modified_at"] = frame["modified_at"].map(
        lambda value: datetime.fromtimestamp(value).strftime("%Y-%m-%d %H:%M:%S")
    )
    return frame[["name", "relative_path", "size_mb", "modified_at"]]


def _ensure_segment_columns(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return frame
    normalized = frame.copy()
    if "segment_type" not in normalized.columns:
        normalized["segment_type"] = "system_total"
    if "segment_id" not in normalized.columns:
        normalized["segment_id"] = "all"
    normalized["segment_id"] = normalized["segment_id"].astype(str)
    return normalized


def _split_diagnostics_key(key: str, summary_row: dict[str, Any]) -> tuple[str, str, str]:
    if "__" in key:
        parts = key.split("__", 2)
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]
    return (
        str(summary_row.get("frequency", key)),
        str(summary_row.get("segment_type", "system_total")),
        str(summary_row.get("segment_id", "all")),
    )


def _build_diagnostics_frame(summary: dict[str, Any]) -> pd.DataFrame:
    diagnostics = summary.get("diagnostics_summary", {}) if summary else {}
    if not diagnostics:
        return pd.DataFrame()

    rows: list[dict[str, Any]] = []
    for key, payload in diagnostics.items():
        row = dict(payload)
        frequency, segment_type, segment_id = _split_diagnostics_key(key, row)
        row["diagnostics_key"] = key
        row["frequency"] = frequency
        row["segment_type"] = segment_type
        row["segment_id"] = segment_id
        rows.append(row)
    return pd.DataFrame(rows).sort_values(["frequency", "segment_type", "segment_id"]).reset_index(drop=True)


def _build_interval_view(
    forecast_outputs: pd.DataFrame,
    forecast_intervals: pd.DataFrame,
    frequency: str,
    model_name: str,
    segment_type: str,
    segment_id: str,
) -> pd.DataFrame:
    if forecast_outputs.empty or forecast_intervals.empty:
        return pd.DataFrame()

    filtered_outputs = forecast_outputs.loc[
        (forecast_outputs["frequency"] == frequency)
        & (forecast_outputs["model_name"] == model_name)
        & (forecast_outputs["segment_type"] == segment_type)
        & (forecast_outputs["segment_id"] == segment_id)
    ].copy()
    if filtered_outputs.empty:
        return pd.DataFrame()

    filtered_intervals = forecast_intervals.loc[
        forecast_intervals["forecast_id"].isin(filtered_outputs["forecast_id"])
    ].copy()
    if filtered_intervals.empty:
        return filtered_outputs

    pivot = filtered_intervals.pivot(index="forecast_id", columns="interval_level", values=["lower_bound", "upper_bound"])
    pivot.columns = [f"{bound}_{int(level)}" for bound, level in pivot.columns]
    merged = filtered_outputs.merge(pivot.reset_index(), on="forecast_id", how="left")
    return merged.sort_values("target_timestamp").reset_index(drop=True)


def _render_story_cards(st, summary: dict[str, Any], forecast_outputs: pd.DataFrame) -> None:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown(
            """
            <div class="story-card">
              <div class="story-card-title">Who This Dashboard Is For</div>
              <div class="story-card-body">
                Operations managers, fleet planners, and analytics leads who need to understand how demand behaves,
                which models are trustworthy, and where forecast uncertainty is highest.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col2:
        st.markdown(
            """
            <div class="story-card">
              <div class="story-card-title">What The System Answers</div>
              <div class="story-card-body">
                It separates historical model evaluation from future forecasting, tracks probabilistic intervals,
                and highlights regime-aware demand shifts instead of pretending the pandemic was ordinary seasonality.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    with col3:
        run_config = summary.get("run_configuration", {}) if summary else {}
        available_frequencies = sorted(forecast_outputs["frequency"].dropna().unique().tolist()) if not forecast_outputs.empty else []
        st.markdown(
            f"""
            <div class="story-card">
              <div class="story-card-title">Current Run Scope</div>
              <div class="story-card-body">
                Frequencies with saved forecasts: {", ".join(available_frequencies) if available_frequencies else "none yet"}.
                Max backtest folds: {run_config.get("max_backtest_folds", "n/a")}. Station-level top-N modeling: {run_config.get("station_level_top_n", 0)}.
              </div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def _backtest_explanation(summary: dict[str, Any], frequency: str, settings) -> str:
    run_config = summary.get("run_configuration", {}) if summary else {}
    horizon = run_config.get("horizon_map", {}).get(frequency, settings.horizon_for(frequency))
    step = run_config.get("step_map", {}).get(frequency, settings.step_for(frequency))
    initial_window = run_config.get("initial_window_map", {}).get(frequency, settings.initial_window_for(frequency))
    max_folds = run_config.get("max_backtest_folds", settings.max_backtest_folds)
    return (
        f"A backtest fold is one rolling holdout window. For {frequency} demand, this run trains on the first "
        f"{initial_window} periods, predicts the next {horizon} periods, then shifts forward by {step} periods. "
        f"`Max backtest folds = {max_folds}` means the dashboard evaluates up to {max_folds} separate time-based windows, "
        "not random splits and not a unit like days by itself."
    )


def _format_segment_label(segment_type: str, segment_id: str) -> str:
    if segment_type == "system_total":
        return "System total"
    if segment_type == "start_station":
        return f"Start station {segment_id}"
    return f"{segment_type}: {segment_id}"


def _render_overview(st, context: dict[str, Any], settings) -> None:
    summary = context["summary"]
    if not summary:
        st.info("No completed pipeline summary was found yet. Run the pipeline from the sidebar to populate the dashboard.")
        return

    forecast_outputs = _ensure_segment_columns(context["forecast_outputs"])
    backtest_summary = _ensure_segment_columns(context["backtest_summary"])
    validation_summary = _ensure_segment_columns(context.get("validation_summary", pd.DataFrame()))
    test_summary = _ensure_segment_columns(context.get("test_summary", pd.DataFrame()))
    diagnostics_frame = _build_diagnostics_frame(summary)
    champions = pd.DataFrame(summary.get("champions", []))
    champions = _ensure_segment_columns(champions)
    station_profiles = context.get("station_profiles", pd.DataFrame())
    reconciliation = context.get("reconciliation_outputs", pd.DataFrame())
    cleaning = summary.get("cleaning_summary", {})

    _render_story_cards(st, summary, forecast_outputs)
    st.markdown("")

    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Raw Records", f"{cleaning.get('records_raw', 0):,}")
    col2.metric("Cleaned Records", f"{cleaning.get('records_cleaned', 0):,}")
    col3.metric("Duplicates Removed", f"{cleaning.get('duplicates_removed', 0):,}")
    col4.metric("Champion Models", f"{len(champions):,}")

    if not forecast_outputs.empty and len(forecast_outputs["frequency"].dropna().unique()) == 1:
        st.markdown(
            '<div class="section-note">This latest run currently contains one forecast frequency only. '
            "Run the pipeline with more frequencies to populate hourly, weekly, monthly, and quarterly outputs.</div>",
            unsafe_allow_html=True,
        )

    st.subheader("Champion Decisions")
    if champions.empty:
        st.warning("No champion models were found in the latest summary.")
    else:
        champions = champions.copy()
        champions["segment_label"] = champions.apply(lambda row: _format_segment_label(row["segment_type"], row["segment_id"]), axis=1)
        display_columns = [
            column
            for column in [
                "segment_label",
                "frequency",
                "model_name",
                "mae",
                "rmse",
                "smape",
                "coverage_80",
                "composite_score",
                "selection_reason",
            ]
            if column in champions.columns
        ]
        st.dataframe(champions[display_columns], width="stretch", hide_index=True)

    st.subheader("What The Latest Diagnostics Say")
    if diagnostics_frame.empty:
        st.warning("No diagnostics summary found.")
    else:
        primary_story = diagnostics_frame.loc[
            (diagnostics_frame["frequency"] == "daily")
            & (diagnostics_frame["segment_type"] == "system_total")
            & (diagnostics_frame["segment_id"] == "all")
        ]
        if primary_story.empty:
            primary_story = diagnostics_frame.head(1)
        story_row = primary_story.iloc[0]
        insights = story_row.get("insights", [])
        if isinstance(insights, str):
            insights = [insights]
        for insight in insights[:5]:
            st.markdown(f"- {insight}")

        st.dataframe(
            diagnostics_frame[
                [
                    column
                    for column in [
                        "frequency",
                        "segment_type",
                        "segment_id",
                        "row_count",
                        "missing_periods",
                        "zero_share",
                        "lag1_autocorrelation",
                        "seasonal_lag_autocorrelation",
                        "adf_pvalue",
                    ]
                    if column in diagnostics_frame.columns
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    st.subheader("Model Ranking Snapshot")
    if backtest_summary.empty:
        st.warning("No backtest summary artifacts found yet.")
    else:
        ranked = backtest_summary.sort_values(["segment_type", "segment_id", "frequency", "composite_score"]).reset_index(drop=True)
        ranked["segment_label"] = ranked.apply(lambda row: _format_segment_label(row["segment_type"], row["segment_id"]), axis=1)
        st.dataframe(
            ranked[
                [
                    column
                    for column in [
                        "segment_label",
                        "frequency",
                        "model_name",
                        "mae",
                        "rmse",
                        "smape",
                        "coverage_80",
                        "composite_score",
                    ]
                    if column in ranked.columns
                ]
            ],
            width="stretch",
            hide_index=True,
        )

        system_ranked = ranked.loc[(ranked["segment_type"] == "system_total") & (ranked["segment_id"] == "all")]
        if not system_ranked.empty:
            chart_frame = system_ranked[["model_name", "composite_score"]].set_index("model_name")
            st.bar_chart(chart_frame, width="stretch")

    st.subheader("Promotion And Final Test Check")
    champion_test_view = pd.DataFrame()
    if not champions.empty and not test_summary.empty:
        champion_test_view = champions.merge(
            test_summary[
                [
                    column
                    for column in [
                        "model_name",
                        "frequency",
                        "segment_type",
                        "segment_id",
                        "mae",
                        "rmse",
                        "smape",
                        "mase",
                        "coverage_80",
                        "coverage_95",
                        "bias",
                    ]
                    if column in test_summary.columns
                ]
            ],
            on=["model_name", "frequency", "segment_type", "segment_id"],
            how="left",
            suffixes=("_validation", "_test"),
        )
    if champion_test_view.empty:
        st.info("Validation and final test comparison is not available yet for this run.")
    else:
        champion_test_view["segment_label"] = champion_test_view.apply(
            lambda row: _format_segment_label(row["segment_type"], row["segment_id"]),
            axis=1,
        )
        st.dataframe(
            champion_test_view[
                [
                    column
                    for column in [
                        "segment_label",
                        "frequency",
                        "model_name",
                        "mae_validation",
                        "rmse_validation",
                        "smape_validation",
                        "mae_test",
                        "rmse_test",
                        "smape_test",
                        "coverage_80_test",
                        "coverage_95_test",
                        "selection_reason",
                    ]
                    if column in champion_test_view.columns
                ]
            ],
            width="stretch",
            hide_index=True,
        )

    if not station_profiles.empty:
        st.subheader("Station Modeling Coverage")
        coverage = (
            station_profiles.groupby(["frequency", "modeling_strategy"], as_index=False)["station_id"]
            .count()
            .rename(columns={"station_id": "station_count"})
        )
        st.dataframe(coverage, width="stretch", hide_index=True)

    if not reconciliation.empty:
        st.subheader("Total vs Station Coherence")
        coherence = pd.DataFrame(
            [
                {
                    "rows": len(reconciliation),
                    "avg_direct_station_modeled_sum": round(float(reconciliation["direct_station_modeled_sum"].mean()), 2),
                    "avg_unmodeled_station_allocated_sum": round(float(reconciliation["unmodeled_station_allocated_sum"].mean()), 2),
                    "avg_reconciliation_scale_factor": round(float(reconciliation["reconciliation_scale_factor"].mean()), 4),
                }
            ]
        )
        st.dataframe(coherence, width="stretch", hide_index=True)


def _render_evaluation(st, context: dict[str, Any], settings) -> None:
    summary = context["summary"]
    backtest_summary = _ensure_segment_columns(context["backtest_summary"])
    validation_summary = _ensure_segment_columns(context.get("validation_summary", pd.DataFrame()))
    test_summary = _ensure_segment_columns(context.get("test_summary", pd.DataFrame()))
    evaluation_predictions = _ensure_segment_columns(context.get("evaluation_predictions", pd.DataFrame()))
    backtest_predictions = _ensure_segment_columns(context["backtest_predictions"])
    backtest_folds = _ensure_segment_columns(context["backtest_folds"])

    available_roles = []
    if not backtest_summary.empty and not backtest_predictions.empty:
        available_roles.append("rolling_backtest")
    if not validation_summary.empty and not evaluation_predictions.empty:
        available_roles.append("validation")
    if not test_summary.empty and not evaluation_predictions.empty:
        available_roles.append("test")
    if not available_roles:
        st.info("Evaluation artifacts are not available yet for this run. Run the pipeline again to populate rolling, validation, and test outputs.")
        return

    selected_role = st.selectbox(
        "Evaluation window",
        options=available_roles,
        format_func=lambda value: {
            "rolling_backtest": "Rolling Backtest (development only)",
            "validation": "Validation (used for model selection)",
            "test": "Final Test (promotion check)",
        }[value],
        key="eval_window_role",
    )

    if selected_role == "rolling_backtest":
        active_summary = backtest_summary
        active_predictions = backtest_predictions
    elif selected_role == "validation":
        active_summary = validation_summary
        active_predictions = evaluation_predictions.loc[evaluation_predictions["window_role"] == "validation"].copy()
    else:
        active_summary = test_summary
        active_predictions = evaluation_predictions.loc[evaluation_predictions["window_role"] == "test"].copy()

    available_frequencies = sorted(active_summary["frequency"].dropna().unique().tolist())
    selected_frequency = st.selectbox("Evaluation frequency", available_frequencies, key="eval_frequency")
    if selected_role == "rolling_backtest":
        explanation = _backtest_explanation(summary, selected_frequency, settings)
    elif selected_role == "validation":
        explanation = (
            "Validation is the model-selection window. Each challenger is trained only on the earlier training slice, "
            "then evaluated on this holdout before any final promotion."
        )
    else:
        explanation = (
            "The final test window is held back from model selection. Use it to judge whether the promoted models still hold up after selection."
        )
    st.markdown(f'<div class="section-note">{explanation}</div>', unsafe_allow_html=True)

    freq_summary = active_summary.loc[active_summary["frequency"] == selected_frequency].copy()
    segment_options = (
        freq_summary[["segment_type", "segment_id"]]
        .drop_duplicates()
        .apply(lambda row: _format_segment_label(row["segment_type"], row["segment_id"]), axis=1)
        .tolist()
    )
    segment_lookup = {
        _format_segment_label(row.segment_type, row.segment_id): (row.segment_type, str(row.segment_id))
        for row in freq_summary[["segment_type", "segment_id"]].drop_duplicates().itertuples()
    }
    selected_segment_label = st.selectbox("Evaluation segment", segment_options, key="eval_segment")
    segment_type, segment_id = segment_lookup[selected_segment_label]

    filtered_summary = freq_summary.loc[
        (freq_summary["segment_type"] == segment_type) & (freq_summary["segment_id"].astype(str) == segment_id)
    ].sort_values("composite_score")
    st.subheader("Held-Out Model Ranking")
    st.dataframe(filtered_summary, width="stretch", hide_index=True)

    filtered_predictions = active_predictions.loc[
        (active_predictions["frequency"] == selected_frequency)
        & (active_predictions["segment_type"] == segment_type)
        & (active_predictions["segment_id"].astype(str) == segment_id)
    ].copy()
    if filtered_predictions.empty:
        st.warning("No evaluation predictions were found for the selected segment.")
        return

    selected_models = st.multiselect(
        "Models to compare on held-out data",
        sorted(filtered_predictions["model_name"].dropna().unique().tolist()),
        default=filtered_summary["model_name"].head(min(3, len(filtered_summary))).tolist(),
        key="eval_models",
    )
    if not selected_models:
        st.warning("Select at least one model to compare.")
        return

    if selected_role == "rolling_backtest":
        fold_values = sorted(filtered_predictions["fold_id"].dropna().unique().tolist())
        default_fold = fold_values[-1]
        selected_fold = st.selectbox("Backtest fold", fold_values, index=fold_values.index(default_fold), key="eval_fold")

        fold_meta = backtest_folds.loc[
            (backtest_folds["frequency"] == selected_frequency)
            & (backtest_folds["segment_type"] == segment_type)
            & (backtest_folds["segment_id"].astype(str) == segment_id)
            & (backtest_folds["fold_id"] == selected_fold)
        ]
        if not fold_meta.empty:
            row = fold_meta.iloc[0]
            st.caption(
                "Fold window: "
                f"train {row['training_window_start'].date()} to {row['training_window_end'].date()}, "
                f"test {row['test_window_start'].date()} to {row['test_window_end'].date()}."
            )
        display_predictions = filtered_predictions.loc[
            (filtered_predictions["fold_id"] == selected_fold)
            & (filtered_predictions["model_name"].isin(selected_models))
        ].copy()
    else:
        first_row = filtered_predictions.iloc[0]
        st.caption(
            "Window: "
            f"train {pd.to_datetime(first_row['training_window_start']).date()} to {pd.to_datetime(first_row['training_window_end']).date()}, "
            f"holdout {pd.to_datetime(first_row['holdout_window_start']).date()} to {pd.to_datetime(first_row['holdout_window_end']).date()}."
        )
        display_predictions = filtered_predictions.loc[
            filtered_predictions["model_name"].isin(selected_models)
        ].copy()

    plot_frame = display_predictions.pivot(index="target_timestamp", columns="model_name", values="prediction").sort_index()
    actual_series = (
        display_predictions[["target_timestamp", "actual"]]
        .drop_duplicates()
        .sort_values("target_timestamp")
        .set_index("target_timestamp")
        .rename(columns={"actual": "actual"})
    )
    plot_frame = actual_series.join(plot_frame, how="outer")

    st.subheader("Out-of-Sample Actual vs Predicted")
    st.line_chart(plot_frame, width="stretch")

    st.subheader("Prediction Table")
    st.dataframe(
        display_predictions[
            [
                column
                for column in [
                    "target_timestamp",
                    "model_name",
                    "actual",
                    "prediction",
                    "lower_80",
                    "upper_80",
                    "evaluation_regime",
                    "horizon_step",
                ]
                if column in display_predictions.columns
            ]
        ].sort_values(["target_timestamp", "model_name"]),
        width="stretch",
        hide_index=True,
    )


def _render_forecasts(st, context: dict[str, Any]) -> None:
    forecast_outputs = _ensure_segment_columns(context["forecast_outputs"])
    forecast_intervals = context["forecast_intervals"].copy()
    model_registry = _ensure_segment_columns(context["model_registry"])
    aggregate_frames = context["aggregate_frames"]

    st.markdown(
        '<div class="section-note">This tab shows future forecasts after the selected models were refit on all available history. '
        "Use the Evaluation tab to judge held-out performance. Use this tab to compare the final production-style forecasts.</div>",
        unsafe_allow_html=True,
    )

    if forecast_outputs.empty:
        st.info("No local forecast outputs were found yet. Run the pipeline to generate forecast CSV artifacts.")
        return

    if not model_registry.empty:
        st.subheader("Forecast Model Coverage")
        st.dataframe(
            model_registry[
                [
                    column
                    for column in [
                        "model_name",
                        "model_family",
                        "frequency",
                        "segment_type",
                        "segment_id",
                        "horizon",
                        "training_window_start",
                        "training_window_end",
                        "trained_at",
                    ]
                    if column in model_registry.columns
                ]
            ].sort_values(["frequency", "segment_type", "segment_id", "model_name"]),
            width="stretch",
            hide_index=True,
        )

    available_frequencies = sorted(forecast_outputs["frequency"].dropna().unique().tolist())
    selected_frequency = st.selectbox("Forecast frequency", available_frequencies, key="forecast_frequency")
    frequency_forecasts = forecast_outputs.loc[forecast_outputs["frequency"] == selected_frequency].copy()

    segment_options = (
        frequency_forecasts[["segment_type", "segment_id"]]
        .drop_duplicates()
        .apply(lambda row: _format_segment_label(row["segment_type"], row["segment_id"]), axis=1)
        .tolist()
    )
    segment_lookup = {
        _format_segment_label(row.segment_type, row.segment_id): (row.segment_type, str(row.segment_id))
        for row in frequency_forecasts[["segment_type", "segment_id"]].drop_duplicates().itertuples()
    }
    selected_segment_label = st.selectbox("Forecast segment", segment_options, key="forecast_segment")
    segment_type, segment_id = segment_lookup[selected_segment_label]
    frequency_forecasts = frequency_forecasts.loc[
        (frequency_forecasts["segment_type"] == segment_type)
        & (frequency_forecasts["segment_id"].astype(str) == segment_id)
    ].copy()

    available_models = sorted(frequency_forecasts["model_name"].dropna().unique().tolist())
    selected_models = st.multiselect(
        "Models to compare",
        available_models,
        default=available_models[: min(len(available_models), 4)],
        key="forecast_models",
    )
    if not selected_models:
        st.warning("Select at least one model to visualize forecasts.")
        return

    chart_frame = (
        frequency_forecasts.loc[frequency_forecasts["model_name"].isin(selected_models), ["target_timestamp", "model_name", "prediction"]]
        .pivot(index="target_timestamp", columns="model_name", values="prediction")
        .sort_index()
    )
    st.subheader("Future Point Forecast Comparison")
    st.line_chart(chart_frame, width="stretch")

    selected_model = st.selectbox("Model for interval details", selected_models, key="forecast_interval_model")
    interval_view = _build_interval_view(
        forecast_outputs,
        forecast_intervals,
        selected_frequency,
        selected_model,
        segment_type,
        segment_id,
    )
    if interval_view.empty:
        st.warning("No forecast interval details found for the selected model.")
        return

    interval_columns = [
        column
        for column in [
            "target_timestamp",
            "prediction",
            "lower_bound_50",
            "upper_bound_50",
            "lower_bound_80",
            "upper_bound_80",
            "lower_bound_95",
            "upper_bound_95",
            "horizon",
        ]
        if column in interval_view.columns
    ]
    st.subheader("Forecast Interval Table")
    st.dataframe(interval_view[interval_columns], width="stretch", hide_index=True)

    aggregate_frame = aggregate_frames.get(selected_frequency, pd.DataFrame()).copy()
    if not aggregate_frame.empty:
        actuals = aggregate_frame.loc[
            (aggregate_frame["segment_type"] == segment_type) & (aggregate_frame["segment_id"].astype(str) == segment_id),
            ["bucket_start", "trip_count"],
        ].sort_values("bucket_start")
        if not actuals.empty:
            max_history = min(len(actuals), 180)
            min_history = min(14, max_history)
            default_history = min(90, max_history)
            history_points = st.slider(
                "Recent actual periods to show",
                min_value=min_history,
                max_value=max_history,
                value=default_history,
                step=1,
                key="forecast_history_points",
            )
            recent_actuals = actuals.tail(history_points).rename(columns={"bucket_start": "timestamp", "trip_count": "actual"})
            selected_forecast = interval_view[["target_timestamp", "prediction"]].rename(
                columns={"target_timestamp": "timestamp", "prediction": selected_model}
            )
            combined_chart = recent_actuals.merge(selected_forecast, on="timestamp", how="outer").sort_values("timestamp")
            combined_chart = combined_chart.set_index("timestamp")
            st.subheader("Recent Actuals vs Selected Forecast")
            st.line_chart(combined_chart, width="stretch")


def _render_diagnostics(st, context: dict[str, Any]) -> None:
    diagnostics_frame = _build_diagnostics_frame(context["summary"])
    image_map = context["diagnostic_images"]

    if diagnostics_frame.empty:
        st.info("No diagnostic images found yet. Run the pipeline first.")
        return

    selected_frequency = st.selectbox(
        "Diagnostics frequency",
        sorted(diagnostics_frame["frequency"].dropna().unique().tolist()),
        key="diagnostics_frequency",
    )
    frequency_rows = diagnostics_frame.loc[diagnostics_frame["frequency"] == selected_frequency].copy()
    segment_options = frequency_rows.apply(lambda row: _format_segment_label(row["segment_type"], row["segment_id"]), axis=1).tolist()
    segment_lookup = {
        _format_segment_label(row.segment_type, row.segment_id): row.diagnostics_key
        for row in frequency_rows.itertuples()
    }
    selected_segment = st.selectbox("Diagnostics segment", segment_options, key="diagnostics_segment")
    selected_key = segment_lookup[selected_segment]

    selected_row = diagnostics_frame.loc[diagnostics_frame["diagnostics_key"] == selected_key].iloc[0]
    st.subheader("Diagnostic Story")
    insights = selected_row.get("insights", [])
    if isinstance(insights, str):
        insights = [insights]
    for insight in insights:
        st.markdown(f"- {insight}")

    st.dataframe(
        pd.DataFrame(
            [
                {
                    "row_count": selected_row.get("row_count"),
                    "missing_periods": selected_row.get("missing_periods"),
                    "zero_share": selected_row.get("zero_share"),
                    "lag1_autocorrelation": selected_row.get("lag1_autocorrelation"),
                    "seasonal_lag_autocorrelation": selected_row.get("seasonal_lag_autocorrelation"),
                    "adf_pvalue": selected_row.get("adf_pvalue"),
                    "dominant_periods": selected_row.get("dominant_periods"),
                }
            ]
        ),
        width="stretch",
        hide_index=True,
    )

    if selected_key in image_map:
        st.subheader("Diagnostic Plots")
        for image_path in image_map[selected_key]:
            st.image(str(image_path), caption=PLOT_EXPLANATIONS.get(image_path.name, image_path.name), width="stretch")
    else:
        st.warning("No images were found for the selected diagnostics segment.")


def _render_segment_explorer(st, context: dict[str, Any]) -> None:
    aggregate_frames = context["aggregate_frames"]
    station_coordinates = context.get("station_coordinates", pd.DataFrame()).copy()
    if not aggregate_frames:
        st.info("No processed aggregate files are available yet.")
        return

    selected_frequency = st.selectbox("Explorer frequency", sorted(aggregate_frames.keys()), key="segment_frequency")
    aggregate_frame = aggregate_frames[selected_frequency].copy()
    aggregate_frame["segment_id"] = aggregate_frame["segment_id"].astype(str)

    top_station_totals = (
        aggregate_frame.loc[aggregate_frame["segment_type"] == "start_station"]
        .groupby("segment_id", as_index=False)["trip_count"]
        .sum()
        .sort_values("trip_count", ascending=False)
        .head(15)
    )

    st.subheader("Top Station Demand Snapshot")
    map_frame = pd.DataFrame()
    if not top_station_totals.empty and not station_coordinates.empty:
        map_frame = top_station_totals.copy()
        map_frame["segment_id"] = map_frame["segment_id"].astype(str)
        map_frame = map_frame.merge(
            station_coordinates.rename(columns={"station_id": "segment_id"}),
            on="segment_id",
            how="left",
        ).dropna(subset=["latitude", "longitude"])

    if not map_frame.empty:
        import pydeck as pdk

        scale = max(float(map_frame["trip_count"].max()), 1.0)
        map_frame["radius"] = map_frame["trip_count"].map(lambda value: 120 + 700 * (float(value) / scale))
        map_frame["station_label"] = "Station " + map_frame["segment_id"].astype(str)
        view_state = pdk.ViewState(
            latitude=float(map_frame["latitude"].mean()),
            longitude=float(map_frame["longitude"].mean()),
            zoom=11.3,
            pitch=28,
        )
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_frame,
            get_position="[longitude, latitude]",
            get_radius="radius",
            get_fill_color="[24, 128, 84, 170]",
            get_line_color="[10, 60, 37, 220]",
            line_width_min_pixels=1,
            pickable=True,
            stroked=True,
        )
        tooltip = {
            "html": "<b>{station_label}</b><br/>Trips: {trip_count}",
            "style": {
                "backgroundColor": "#163b2a",
                "color": "white",
            },
        }
        st.pydeck_chart(
            pdk.Deck(
                layers=[layer],
                initial_view_state=view_state,
                tooltip=tooltip,
                map_style="light",
            )
        )
        st.caption("Circle size represents cumulative demand for the selected frequency across the top stations.")
    elif top_station_totals.empty:
        st.info("This frequency does not currently have station-level aggregate rows.")
    else:
        st.info("Station demand exists, but station coordinates were not available for map rendering.")

    if not top_station_totals.empty:
        st.dataframe(top_station_totals.rename(columns={"segment_id": "start_station"}), width="stretch", hide_index=True)

    scope = st.radio("Segment scope", ["system_total", "start_station"], horizontal=True)
    if scope == "system_total":
        segment_id = "all"
    else:
        station_ids = sorted(aggregate_frame.loc[aggregate_frame["segment_type"] == "start_station", "segment_id"].unique().tolist())
        if not station_ids:
            st.warning("No station-level rows are available for this frequency.")
            return
        segment_id = st.selectbox("Start station ID", station_ids, key="segment_station_id")

    subset = aggregate_frame.loc[
        (aggregate_frame["segment_type"] == scope) & (aggregate_frame["segment_id"] == segment_id)
    ].sort_values("bucket_start")
    if subset.empty:
        st.warning("No aggregate history was found for the selected segment.")
        return

    st.subheader("Historical Demand Profile")
    history_chart = subset[["bucket_start", "trip_count"]].rename(columns={"bucket_start": "timestamp"}).set_index("timestamp")
    st.line_chart(history_chart, width="stretch")

    stats = {
        "segment": _format_segment_label(scope, segment_id),
        "rows": len(subset),
        "average_trip_count": round(float(subset["trip_count"].mean()), 2),
        "max_trip_count": int(subset["trip_count"].max()),
        "min_trip_count": int(subset["trip_count"].min()),
    }
    st.dataframe(pd.DataFrame([stats]), width="stretch", hide_index=True)

    st.markdown(
        '<div class="section-note">Station-level history is available immediately because the warehouse stores start-station aggregates. '
        "In v2, high-priority stations can be modeled directly while the rest stay in the hierarchy through share allocation and reconciliation.</div>",
        unsafe_allow_html=True,
    )


def _render_artifacts(st, context: dict[str, Any]) -> None:
    st.subheader("Processed Data")
    st.dataframe(_artifact_frame(context["processed_artifacts"]), width="stretch", hide_index=True)

    st.subheader("Reports")
    st.dataframe(_artifact_frame(context["report_artifacts"]), width="stretch", hide_index=True)

    st.subheader("Forecast Outputs")
    st.dataframe(_artifact_frame(context["forecast_artifacts"]), width="stretch", hide_index=True)


def main() -> None:
    try:
        import streamlit as st
    except ModuleNotFoundError as exc:  # pragma: no cover - exercised only when dependency missing
        raise SystemExit(
            "Streamlit is not installed. Run `python scripts/bootstrap.py --prepare` or `pip install -r requirements.txt`."
        ) from exc

    settings = get_settings()
    context = build_dashboard_context(settings.project_root)

    st.set_page_config(
        page_title="Metro Bike Share Forecasting Studio",
        page_icon=":bar_chart:",
        layout="wide",
    )
    _apply_green_theme(st)

    st.title("Metro Bike Share Forecasting Studio")
    st.caption("Adaptive probabilistic forecasting and demand diagnostics for operations, planning, and fleet strategy.")

    with st.sidebar:
        st.header("Run Controls")
        selected_frequencies = st.multiselect(
            "Frequencies",
            options=["hourly", "daily", "weekly", "monthly", "quarterly"],
            default=list(settings.frequencies),
            help="These are the forecast granularities to compute on the next run.",
        )
        max_backtest_folds = st.number_input(
            "Max Backtest Folds",
            min_value=1,
            max_value=24,
            value=settings.max_backtest_folds,
            step=1,
            help="A fold is one rolling time-based evaluation window. More folds mean broader historical testing but slower runs.",
        )
        persist_to_postgres = st.checkbox(
            "Persist to PostgreSQL",
            value=bool(settings.postgres_url),
            help="Turn this off for a file-only local run if PostgreSQL is not running.",
        )
        if st.button("Run Pipeline", width="stretch"):
            with st.spinner("Running forecasting pipeline..."):
                result = _run_pipeline(selected_frequencies or ["daily"], int(max_backtest_folds), persist_to_postgres)
            if result["returncode"] == 0:
                st.success("Pipeline completed. Refreshing dashboard artifacts.")
                with st.expander("Pipeline stdout", expanded=False):
                    st.code(result["stdout"] or "No stdout emitted.", language="text")
                st.rerun()
            else:
                st.error(f"Pipeline exited with code {result['returncode']}.")
                with st.expander("Pipeline stdout", expanded=False):
                    st.code(result["stdout"] or "No stdout emitted.", language="text")
                if result["stderr"]:
                    with st.expander("Pipeline stderr", expanded=True):
                        st.code(result["stderr"], language="text")

        st.divider()
        st.header("Project Paths")
        st.write(f"Project root: `{settings.project_root}`")
        st.write(f"Processed data: `{settings.processed_dir}`")
        st.write(f"Reports: `{settings.outputs_reports_dir}`")
        st.write(f"Figures: `{settings.outputs_figures_dir}`")
        st.write(f"Forecasts: `{settings.outputs_forecasts_dir}`")

    latest_summary_path = context.get("summary_path")
    if latest_summary_path:
        st.caption(f"Latest run summary: `{latest_summary_path}`")

    overview_tab, evaluation_tab, forecasts_tab, diagnostics_tab, explorer_tab, artifacts_tab = st.tabs(
        ["Overview", "Evaluation", "Forecasts", "Diagnostics", "Segment Explorer", "Artifacts"]
    )
    with overview_tab:
        _render_overview(st, context, settings)
    with evaluation_tab:
        _render_evaluation(st, context, settings)
    with forecasts_tab:
        _render_forecasts(st, context)
    with diagnostics_tab:
        _render_diagnostics(st, context)
    with explorer_tab:
        _render_segment_explorer(st, context)
    with artifacts_tab:
        _render_artifacts(st, context)


if __name__ == "__main__":
    main()
