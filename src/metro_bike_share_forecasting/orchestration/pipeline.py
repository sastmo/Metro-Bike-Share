from __future__ import annotations

import json
from pathlib import Path
from typing import Callable

import pandas as pd

from metro_bike_share_forecasting.cleaning.cleaner import clean_trip_data
from metro_bike_share_forecasting.cleaning.legacy_rules import describe_legacy_reuse
from metro_bike_share_forecasting.config.settings import Settings
from metro_bike_share_forecasting.database.repository import PostgresRepository
from metro_bike_share_forecasting.diagnostics.time_series import run_diagnostics
from metro_bike_share_forecasting.evaluation.backtesting import (
    BacktestConfig,
    describe_rolling_folds,
    evaluate_holdout,
    run_backtest,
)
from metro_bike_share_forecasting.evaluation.splitting import build_temporal_split, describe_temporal_split
from metro_bike_share_forecasting.features.aggregation import build_multigranularity_aggregates
from metro_bike_share_forecasting.features.engineering import build_feature_store, to_feature_payload
from metro_bike_share_forecasting.features.regime import add_regime_features, derive_regime_definition
from metro_bike_share_forecasting.forecasting.baselines.seasonal_naive import (
    NaiveForecaster,
    RollingMeanForecaster,
    SeasonalNaiveForecaster,
)
from metro_bike_share_forecasting.forecasting.classical.glm_count import CountGLMForecaster
from metro_bike_share_forecasting.forecasting.classical.sarimax_fourier import SarimaxFourierForecaster
from metro_bike_share_forecasting.forecasting.ensemble.weighted_ensemble import (
    combine_backtest_predictions,
    combine_forecasts,
    compute_inverse_error_weights,
)
from metro_bike_share_forecasting.ingestion.csv_loader import discover_csv_files, load_trip_csvs
from metro_bike_share_forecasting.monitoring.monitoring import build_drift_monitoring
from metro_bike_share_forecasting.selection.champion import select_champion_model, summarize_backtests
from metro_bike_share_forecasting.utils.ids import generate_run_id
from metro_bike_share_forecasting.utils.time import infer_season_length
from metro_bike_share_forecasting.validation.quality import build_data_quality_monitoring, build_raw_ingestion_log


MODEL_FAMILY_MAP = {
    "naive": "baseline",
    "seasonal_naive": "baseline",
    "rolling_mean": "baseline",
    "count_glm": "count_glm",
    "sarimax_fourier": "sarimax_fourier",
    "weighted_ensemble": "ensemble",
    "reconciled_station_hybrid": "reconciliation",
}

INTERVAL_LEVELS = (50, 80, 95)


class ForecastingPipeline:
    def __init__(self, settings: Settings, logger) -> None:
        self.settings = settings
        self.logger = logger
        self.repository = (
            PostgresRepository(settings.postgres_url, settings.postgres_schema)
            if settings.postgres_url
            else None
        )

    def inspect_legacy_logic(self) -> dict[str, object]:
        summary = describe_legacy_reuse().as_dict()
        output_path = self.settings.outputs_reports_dir / "legacy_reuse_summary.json"
        output_path.write_text(json.dumps(summary, indent=2))
        return summary

    def _persist_if_configured(self, table_name: str, frame: pd.DataFrame, conflict_columns: list[str]) -> None:
        if self.repository is None or frame.empty:
            return
        self.repository.upsert_dataframe(table_name, frame, conflict_columns)

    def run(self) -> dict[str, object]:
        pipeline_run_id = generate_run_id("forecasting")
        run_started_at = pd.Timestamp.utcnow()
        self.logger.info("Starting forecasting pipeline run %s", pipeline_run_id)

        if self.repository is not None:
            self.repository.initialize_schema(self.settings.sql_schema_path)
            self._persist_pipeline_log(
                pipeline_run_id,
                run_started_at=run_started_at,
                status="running",
                message="Forecasting pipeline started.",
                metadata_json={"frequencies": list(self.settings.frequencies)},
            )

        legacy_summary = self.inspect_legacy_logic()
        trip_files = discover_csv_files(self.settings.raw_trip_dir)
        self.logger.info("Discovered %s trip CSV files", len(trip_files))
        if not trip_files:
            raise FileNotFoundError(f"No CSV files were found in {self.settings.raw_trip_dir}")

        raw_bundle = load_trip_csvs(trip_files)
        cleaning_result = clean_trip_data(raw_bundle.data, self.settings)
        cleaned = cleaning_result.cleaned_data.copy()
        cleaned["pipeline_run_id"] = pipeline_run_id
        raw_columns = [column for column in cleaned.columns if column.startswith("raw_")]
        cleaned["raw_record_json"] = (
            cleaned[raw_columns]
            .replace({pd.NA: None, pd.NaT: None})
            .to_dict(orient="records")
        )
        cleaned.to_csv(self.settings.processed_dir / "cleaned_trip_data.csv.gz", index=False, compression="gzip")

        raw_ingestion_log = build_raw_ingestion_log(raw_bundle.profiles, cleaned, pipeline_run_id)
        dq_monitoring = build_data_quality_monitoring(pipeline_run_id, cleaning_result.quality_summary, raw_bundle.profiles)
        self._persist_if_configured("raw_ingestion_log", raw_ingestion_log, ["pipeline_run_id", "source_file"])
        self._persist_if_configured("data_quality_monitoring", self._prepare_monitoring_rows(dq_monitoring), ["monitoring_id"])

        cleaned_db = cleaned[
            [
                "trip_id",
                "source_file",
                "source_row_number",
                "pipeline_run_id",
                "duration_minutes",
                "start_ts_utc",
                "end_ts_utc",
                "start_ts_local",
                "end_ts_local",
                "start_station",
                "end_station",
                "start_lat",
                "start_lon",
                "end_lat",
                "end_lon",
                "bike_id",
                "plan_duration",
                "trip_route_category",
                "passholder_type",
                "bike_type",
                "event_date_local",
                "event_hour_local",
                "raw_record_json",
            ]
        ].copy()
        cleaned_db["trip_id"] = cleaned_db["trip_id"].astype(str)
        self._persist_if_configured("cleaned_trip_data", cleaned_db, ["trip_id"])

        aggregates = build_multigranularity_aggregates(cleaned, self.settings.frequencies)
        regime_definition = self._derive_regime_definition(aggregates)

        diagnostics_summary: dict[str, object] = {}
        split_rows: list[pd.DataFrame] = []
        fold_schedule_rows: list[pd.DataFrame] = []
        evaluation_metric_rows: list[pd.DataFrame] = []
        evaluation_prediction_rows: list[pd.DataFrame] = []
        backtest_summary_rows: list[pd.DataFrame] = []
        validation_summary_rows: list[pd.DataFrame] = []
        test_summary_rows: list[pd.DataFrame] = []
        champion_rows: list[pd.DataFrame] = []
        forecast_outputs_rows: list[pd.DataFrame] = []
        forecast_interval_rows: list[pd.DataFrame] = []
        model_registry_rows: list[dict[str, object]] = []
        drift_rows: list[pd.DataFrame] = []
        station_profile_rows: list[pd.DataFrame] = []
        reconciliation_rows: list[pd.DataFrame] = []
        forecast_inputs_by_frequency: dict[str, dict[str, object]] = {}

        for frequency, aggregate_frame in aggregates.items():
            self.logger.info("Processing frequency=%s", frequency)
            aggregate_frame = aggregate_frame.copy()
            aggregate_frame["pipeline_run_id"] = pipeline_run_id
            aggregate_frame.to_csv(self.settings.processed_dir / f"{frequency}_aggregate.csv.gz", index=False, compression="gzip")
            self._persist_if_configured(
                f"aggregated_{frequency}",
                aggregate_frame,
                ["bucket_start", "segment_type", "segment_id"],
            )

            feature_store = build_feature_store(
                aggregate_frame,
                frequency,
                regime_definition,
                self.settings.holiday_country,
            )
            feature_store_payload = to_feature_payload(
                feature_store,
                keep_columns=[
                    "bucket_start",
                    "segment_type",
                    "segment_id",
                    "trip_count",
                    "pandemic_phase",
                    "missing_period_flag",
                ],
            ).rename(
                columns={
                    "trip_count": "target_trip_count",
                    "missing_period_flag": "completeness_flag",
                }
            )
            feature_store_payload["pipeline_run_id"] = pipeline_run_id
            feature_store_payload["generated_at"] = pd.Timestamp.utcnow()
            if frequency != "quarterly":
                self._persist_if_configured(
                    f"feature_store_{frequency}",
                    feature_store_payload,
                    ["bucket_start", "segment_type", "segment_id"],
                )

            station_profiles = self._build_station_profiles(aggregate_frame, frequency)
            if not station_profiles.empty:
                station_profiles["pipeline_run_id"] = pipeline_run_id
                station_profile_rows.append(station_profiles)
                self._persist_if_configured(
                    "station_modeling_registry",
                    station_profiles,
                    ["pipeline_run_id", "frequency", "station_id"],
                )

            frequency_forecast_inputs = {
                "station_profiles": station_profiles,
                "direct_station_forecasts": [],
                "total_champion_forecast": None,
            }

            for segment_type, segment_id in self._select_training_segments(aggregate_frame, frequency, station_profiles):
                segment_frame = feature_store.loc[
                    (feature_store["segment_type"] == segment_type)
                    & (feature_store["segment_id"].astype(str) == str(segment_id))
                ].copy()
                if segment_frame.empty:
                    continue

                diagnostics_key = f"{frequency}__{segment_type}__{segment_id}"
                diagnostics_summary[diagnostics_key] = run_diagnostics(
                    segment_frame,
                    diagnostics_key,
                    self.settings.outputs_figures_dir,
                    regime_definition,
                )
                segment_frame = add_regime_features(segment_frame, "bucket_start", regime_definition)

                try:
                    temporal_split = build_temporal_split(
                        segment_frame,
                        frequency=frequency,
                        validation_window=self.settings.validation_window_for(frequency),
                        test_window=self.settings.test_window_for(frequency),
                        minimum_train_window=self.settings.initial_window_for(frequency),
                    )
                except ValueError as exc:
                    self.logger.warning(
                        "Skipping %s/%s=%s because the segment is too short for strict splitting: %s",
                        frequency,
                        segment_type,
                        segment_id,
                        exc,
                    )
                    continue

                split_rows.append(describe_temporal_split(temporal_split, segment_type, str(segment_id)))
                self._persist_if_configured(
                    "split_metadata",
                    describe_temporal_split(temporal_split, segment_type, str(segment_id)).assign(pipeline_run_id=pipeline_run_id),
                    ["pipeline_run_id", "frequency", "segment_type", "segment_id", "window_role"],
                )

                backtest_config = self._build_backtest_config(frequency, temporal_split.train_frame)
                if backtest_config is not None:
                    fold_schedule = describe_rolling_folds(temporal_split.train_frame, backtest_config)
                    if not fold_schedule.empty:
                        fold_schedule["pipeline_run_id"] = pipeline_run_id
                        fold_schedule["segment_type"] = segment_type
                        fold_schedule["segment_id"] = str(segment_id)
                        fold_schedule_rows.append(fold_schedule)

                model_factories = self._build_model_factories(frequency, regime_definition)
                if backtest_config is not None:
                    backtest_predictions, backtest_metrics = run_backtest(temporal_split.train_frame, model_factories, backtest_config)
                else:
                    backtest_predictions, backtest_metrics = pd.DataFrame(), pd.DataFrame()
                if not backtest_metrics.empty:
                    backtest_summary = summarize_backtests(backtest_metrics)
                    backtest_summary["window_role"] = "rolling_backtest"
                    backtest_summary["segment_type"] = segment_type
                    backtest_summary["segment_id"] = str(segment_id)
                    backtest_summary_rows.append(backtest_summary)

                validation_predictions, validation_metrics = evaluate_holdout(
                    temporal_split.train_frame,
                    temporal_split.validation_frame,
                    model_factories,
                    frequency=frequency,
                    window_role="validation",
                )
                validation_base_summary = summarize_backtests(validation_metrics)
                if validation_base_summary.empty:
                    self.logger.warning(
                        "Skipping %s / %s=%s because validation metrics could not be computed.",
                        frequency,
                        segment_type,
                        segment_id,
                    )
                    continue

                ensemble_weights = compute_inverse_error_weights(validation_base_summary)
                season_length = infer_season_length(frequency)
                if ensemble_weights:
                    ensemble_validation_predictions, ensemble_validation_metrics = combine_backtest_predictions(
                        validation_predictions,
                        ensemble_weights,
                        season_length,
                        training_series=temporal_split.train_frame["trip_count"],
                    )
                    validation_predictions = pd.concat(
                        [validation_predictions, ensemble_validation_predictions],
                        ignore_index=True,
                    )
                    validation_metrics = pd.concat(
                        [validation_metrics, ensemble_validation_metrics],
                        ignore_index=True,
                    )

                validation_summary = summarize_backtests(validation_metrics)
                validation_summary["window_role"] = "validation"
                validation_summary["segment_type"] = segment_type
                validation_summary["segment_id"] = str(segment_id)
                validation_summary_rows.append(validation_summary)

                champion = select_champion_model(validation_summary)
                champion["pipeline_run_id"] = pipeline_run_id
                champion["segment_type"] = segment_type
                champion["segment_id"] = str(segment_id)
                champion["updated_at"] = pd.Timestamp.utcnow()
                champion["window_role"] = "validation"
                champion["selection_reason"] = champion.apply(
                    lambda row: (
                        f"Selected {row['model_name']} for {row['frequency']} {segment_type}={segment_id} "
                        f"because it achieved the best validation composite score ({row['composite_score']:.4f}) "
                        "after strict time-based train/validation splitting."
                    ),
                    axis=1,
                )
                champion["metadata_json"] = champion.apply(
                    lambda row: {
                        "ensemble_weights": ensemble_weights,
                        "validation_window_start": str(temporal_split.validation_frame["bucket_start"].min()),
                        "validation_window_end": str(temporal_split.validation_frame["bucket_start"].max()),
                    },
                    axis=1,
                )
                champion_rows.append(champion)
                self._persist_if_configured(
                    "champion_model_registry",
                    champion[
                        [
                            "frequency",
                            "segment_type",
                            "segment_id",
                            "model_name",
                            "pipeline_run_id",
                            "selection_reason",
                            "composite_score",
                            "updated_at",
                            "metadata_json",
                        ]
                    ],
                    ["frequency", "segment_type", "segment_id"],
                )

                test_predictions, test_metrics = evaluate_holdout(
                    temporal_split.train_plus_validation_frame,
                    temporal_split.test_frame,
                    model_factories,
                    frequency=frequency,
                    window_role="test",
                )
                if ensemble_weights:
                    ensemble_test_predictions, ensemble_test_metrics = combine_backtest_predictions(
                        test_predictions,
                        ensemble_weights,
                        season_length,
                        training_series=temporal_split.train_plus_validation_frame["trip_count"],
                    )
                    test_predictions = pd.concat([test_predictions, ensemble_test_predictions], ignore_index=True)
                    test_metrics = pd.concat([test_metrics, ensemble_test_metrics], ignore_index=True)

                test_summary = summarize_backtests(test_metrics)
                test_summary["window_role"] = "test"
                test_summary["segment_type"] = segment_type
                test_summary["segment_id"] = str(segment_id)
                test_summary_rows.append(test_summary)

                evaluation_predictions = [
                    frame
                    for frame in (backtest_predictions, validation_predictions, test_predictions)
                    if not frame.empty
                ]
                if evaluation_predictions:
                    merged_predictions = pd.concat(evaluation_predictions, ignore_index=True)
                    merged_predictions["pipeline_run_id"] = pipeline_run_id
                    merged_predictions["segment_type"] = segment_type
                    merged_predictions["segment_id"] = str(segment_id)
                    merged_predictions["prediction_id"] = [
                        f"{pipeline_run_id}_{frequency}_{segment_type}_{segment_id}_{row.model_name}_{row.window_role}_{row.fold_id}_{row.horizon_step}"
                        for row in merged_predictions.itertuples()
                    ]
                    evaluation_prediction_rows.append(merged_predictions)

                evaluation_metrics = [frame for frame in (backtest_metrics, validation_metrics, test_metrics) if not frame.empty]
                if evaluation_metrics:
                    merged_metrics = pd.concat(evaluation_metrics, ignore_index=True)
                    merged_metrics["pipeline_run_id"] = pipeline_run_id
                    merged_metrics["segment_type"] = segment_type
                    merged_metrics["segment_id"] = str(segment_id)
                    merged_metrics["result_id"] = [
                        f"{pipeline_run_id}_{frequency}_{segment_type}_{segment_id}_{row.model_name}_{row.window_role}_{idx}"
                        for idx, row in enumerate(merged_metrics.itertuples())
                    ]
                    evaluation_metric_rows.append(merged_metrics)
                    self._persist_if_configured("backtest_results", merged_metrics, ["result_id"])

                trained_models = {}
                full_history = pd.concat(
                    [
                        temporal_split.train_frame,
                        temporal_split.validation_frame,
                        temporal_split.test_frame,
                    ],
                    ignore_index=True,
                )
                for model_name, factory in model_factories.items():
                    model = factory()
                    model.fit(full_history)
                    trained_models[model_name] = model
                    model_registry_rows.append(
                        {
                            "model_id": f"{pipeline_run_id}_{frequency}_{segment_type}_{segment_id}_{model_name}",
                            "model_name": model_name,
                            "model_family": MODEL_FAMILY_MAP.get(model_name, model_name),
                            "frequency": frequency,
                            "forecast_level": "station" if segment_type == "start_station" else "total",
                            "horizon": self.settings.horizon_for(frequency),
                            "segment_type": segment_type,
                            "segment_id": str(segment_id),
                            "version": "v2",
                            "training_window_start": temporal_split.train_frame["bucket_start"].min(),
                            "training_window_end": temporal_split.train_frame["bucket_start"].max(),
                            "validation_window_start": temporal_split.validation_frame["bucket_start"].min(),
                            "validation_window_end": temporal_split.validation_frame["bucket_start"].max(),
                            "test_window_start": temporal_split.test_frame["bucket_start"].min(),
                            "test_window_end": temporal_split.test_frame["bucket_start"].max(),
                            "trained_at": pd.Timestamp.utcnow(),
                            "pipeline_run_id": pipeline_run_id,
                            "parameters_json": {"ensemble_weights": ensemble_weights if model_name == "weighted_ensemble" else None},
                        }
                    )

                base_forecasts = {
                    model_name: model.forecast(full_history, self.settings.horizon_for(frequency))
                    for model_name, model in trained_models.items()
                }
                ensemble_forecast = combine_forecasts(base_forecasts, ensemble_weights)
                if not ensemble_forecast.empty:
                    base_forecasts["weighted_ensemble"] = ensemble_forecast
                    model_registry_rows.append(
                        {
                            "model_id": f"{pipeline_run_id}_{frequency}_{segment_type}_{segment_id}_weighted_ensemble",
                            "model_name": "weighted_ensemble",
                            "model_family": "ensemble",
                            "frequency": frequency,
                            "forecast_level": "station" if segment_type == "start_station" else "total",
                            "horizon": self.settings.horizon_for(frequency),
                            "segment_type": segment_type,
                            "segment_id": str(segment_id),
                            "version": "v2",
                            "training_window_start": temporal_split.train_frame["bucket_start"].min(),
                            "training_window_end": temporal_split.train_frame["bucket_start"].max(),
                            "validation_window_start": temporal_split.validation_frame["bucket_start"].min(),
                            "validation_window_end": temporal_split.validation_frame["bucket_start"].max(),
                            "test_window_start": temporal_split.test_frame["bucket_start"].min(),
                            "test_window_end": temporal_split.test_frame["bucket_start"].max(),
                            "trained_at": pd.Timestamp.utcnow(),
                            "pipeline_run_id": pipeline_run_id,
                            "parameters_json": {"ensemble_weights": ensemble_weights},
                        }
                    )

                generated_at = pd.Timestamp.utcnow()
                for model_name, forecast_frame in base_forecasts.items():
                    prepared = self._prepare_forecast_frame(
                        forecast_frame,
                        pipeline_run_id=pipeline_run_id,
                        frequency=frequency,
                        segment_type=segment_type,
                        segment_id=str(segment_id),
                        model_name=model_name,
                        generated_at=generated_at,
                        temporal_split=temporal_split,
                    )
                    forecast_outputs_rows.append(prepared["outputs"])
                    forecast_interval_rows.extend(prepared["intervals"])

                champion_name = champion.iloc[0]["model_name"]
                champion_forecast = base_forecasts[champion_name].copy()
                if segment_type == "system_total" and str(segment_id) == "all":
                    frequency_forecast_inputs["total_champion_forecast"] = champion_forecast
                elif segment_type == "start_station":
                    champion_forecast["segment_id"] = str(segment_id)
                    champion_forecast["model_name"] = champion_name
                    frequency_forecast_inputs["direct_station_forecasts"].append(champion_forecast)

            reconciliation_frame, reconciliation_forecasts = self._build_reconciled_station_outputs(
                frequency=frequency,
                forecast_inputs=frequency_forecast_inputs,
                pipeline_run_id=pipeline_run_id,
            )
            if not reconciliation_frame.empty:
                reconciliation_rows.append(reconciliation_frame)
                self._persist_if_configured(
                    "reconciliation_outputs",
                    reconciliation_frame,
                    ["pipeline_run_id", "frequency", "target_timestamp"],
                )
            if not reconciliation_forecasts.empty:
                prepared = self._prepare_forecast_frame(
                    reconciliation_forecasts,
                    pipeline_run_id=pipeline_run_id,
                    frequency=frequency,
                    segment_type="start_station",
                    segment_id=None,
                    model_name="reconciled_station_hybrid",
                    generated_at=pd.Timestamp.utcnow(),
                    temporal_split=None,
                    per_row_segment=True,
                )
                forecast_outputs_rows.append(prepared["outputs"])
                forecast_interval_rows.extend(prepared["intervals"])

            drift_rows.append(build_drift_monitoring(aggregate_frame, pipeline_run_id, frequency))
            forecast_inputs_by_frequency[frequency] = frequency_forecast_inputs

        split_df = pd.concat(split_rows, ignore_index=True) if split_rows else pd.DataFrame()
        fold_schedule_df = pd.concat(fold_schedule_rows, ignore_index=True) if fold_schedule_rows else pd.DataFrame()
        evaluation_metrics_df = pd.concat(evaluation_metric_rows, ignore_index=True) if evaluation_metric_rows else pd.DataFrame()
        evaluation_predictions_df = pd.concat(evaluation_prediction_rows, ignore_index=True) if evaluation_prediction_rows else pd.DataFrame()
        backtest_summary_df = pd.concat(backtest_summary_rows, ignore_index=True) if backtest_summary_rows else pd.DataFrame()
        validation_summary_df = pd.concat(validation_summary_rows, ignore_index=True) if validation_summary_rows else pd.DataFrame()
        test_summary_df = pd.concat(test_summary_rows, ignore_index=True) if test_summary_rows else pd.DataFrame()
        champion_df = pd.concat(champion_rows, ignore_index=True) if champion_rows else pd.DataFrame()
        forecast_outputs_df = pd.concat(forecast_outputs_rows, ignore_index=True) if forecast_outputs_rows else pd.DataFrame()
        forecast_intervals_df = pd.concat(forecast_interval_rows, ignore_index=True) if forecast_interval_rows else pd.DataFrame()
        model_registry_df = pd.DataFrame(model_registry_rows) if model_registry_rows else pd.DataFrame()
        drift_df = self._prepare_monitoring_rows(pd.concat(drift_rows, ignore_index=True)) if drift_rows else pd.DataFrame()
        station_profiles_df = pd.concat(station_profile_rows, ignore_index=True) if station_profile_rows else pd.DataFrame()
        reconciliation_df = pd.concat(reconciliation_rows, ignore_index=True) if reconciliation_rows else pd.DataFrame()

        self._persist_if_configured("model_registry", model_registry_df, ["model_id"])
        self._persist_if_configured("forecast_outputs", forecast_outputs_df, ["forecast_id"])
        self._persist_if_configured("forecast_intervals", forecast_intervals_df, ["forecast_id", "interval_level"])
        self._persist_if_configured("drift_monitoring", drift_df, ["monitoring_id"])
        self._persist_if_configured("evaluation_predictions", evaluation_predictions_df, ["prediction_id"])

        artifact_paths: dict[str, str] = {
            "cleaned_trip_data": str(self.settings.processed_dir / "cleaned_trip_data.csv.gz"),
            "diagnostics_root": str(self.settings.outputs_figures_dir),
        }
        artifact_paths.update(self._write_artifacts(pipeline_run_id, {
            "split_metadata": split_df,
            "backtest_folds": fold_schedule_df,
            "evaluation_predictions": evaluation_predictions_df,
            "evaluation_metrics": evaluation_metrics_df,
            "backtest_summary": backtest_summary_df,
            "validation_summary": validation_summary_df,
            "test_summary": test_summary_df,
            "champions": champion_df,
            "forecast_outputs": forecast_outputs_df,
            "forecast_intervals": forecast_intervals_df,
            "model_registry": model_registry_df,
            "drift_monitoring": drift_df,
            "station_profiles": station_profiles_df,
            "reconciliation_outputs": reconciliation_df,
            "backtest_predictions": evaluation_predictions_df.loc[evaluation_predictions_df["window_role"] == "rolling_backtest"].copy()
            if not evaluation_predictions_df.empty else pd.DataFrame(),
        }))

        summary_payload = {
            "pipeline_run_id": pipeline_run_id,
            "legacy_reuse": legacy_summary,
            "cleaning_summary": cleaning_result.quality_summary,
            "diagnostics_summary": diagnostics_summary,
            "champions": champion_df.to_dict(orient="records") if not champion_df.empty else [],
            "artifact_paths": artifact_paths,
            "run_configuration": {
                "frequencies": list(self.settings.frequencies),
                "max_backtest_folds": self.settings.max_backtest_folds,
                "horizon_map": {frequency: self.settings.horizon_for(frequency) for frequency in self.settings.frequencies},
                "initial_window_map": {frequency: self.settings.initial_window_for(frequency) for frequency in self.settings.frequencies},
                "validation_window_map": {frequency: self.settings.validation_window_for(frequency) for frequency in self.settings.frequencies},
                "test_window_map": {frequency: self.settings.test_window_for(frequency) for frequency in self.settings.frequencies},
                "step_map": {frequency: self.settings.step_for(frequency) for frequency in self.settings.frequencies},
                "station_level_top_n": self.settings.station_level_top_n,
                "station_level_frequencies": list(self.settings.station_level_frequencies),
            },
        }
        summary_path = self.settings.outputs_reports_dir / f"{pipeline_run_id}_summary.json"
        summary_path.write_text(json.dumps(summary_payload, indent=2, default=str))

        self._persist_pipeline_log(
            pipeline_run_id,
            run_started_at=run_started_at,
            status="completed",
            message="Forecasting pipeline completed successfully.",
            records_ingested=cleaning_result.quality_summary["records_raw"],
            records_cleaned=cleaning_result.quality_summary["records_cleaned"],
            min_event_timestamp=pd.to_datetime(cleaning_result.quality_summary["min_event_timestamp_utc"])
            if cleaning_result.quality_summary["min_event_timestamp_utc"]
            else None,
            max_event_timestamp=pd.to_datetime(cleaning_result.quality_summary["max_event_timestamp_utc"])
            if cleaning_result.quality_summary["max_event_timestamp_utc"]
            else None,
            metadata_json={
                "summary_report": str(summary_path),
                "champion_count": len(summary_payload["champions"]),
            },
        )

        self.logger.info("Completed forecasting pipeline run %s", pipeline_run_id)
        return summary_payload

    def _derive_regime_definition(self, aggregates: dict[str, pd.DataFrame]):
        if "daily" not in aggregates:
            raise ValueError("Daily aggregates are required to derive regime definitions.")
        daily_total = aggregates["daily"].loc[lambda frame: frame["segment_type"] == "system_total"].copy()
        try:
            split = build_temporal_split(
                daily_total,
                frequency="daily",
                validation_window=self.settings.validation_window_for("daily"),
                test_window=self.settings.test_window_for("daily"),
                minimum_train_window=self.settings.initial_window_for("daily"),
            )
            regime_reference = split.train_plus_validation_frame
        except ValueError:
            regime_reference = daily_total
        return derive_regime_definition(regime_reference, self.settings)

    def _build_model_factories(self, frequency: str, regime_definition) -> dict[str, Callable[[], object]]:
        factories = {
            "naive": lambda: NaiveForecaster(frequency),
            "seasonal_naive": lambda: SeasonalNaiveForecaster(frequency),
            "rolling_mean": lambda: RollingMeanForecaster(frequency),
            "count_glm": lambda: CountGLMForecaster(frequency, regime_definition, self.settings.holiday_country),
            "sarimax_fourier": lambda: SarimaxFourierForecaster(frequency, regime_definition, self.settings.holiday_country),
        }
        return {name: factory for name, factory in factories.items() if name in self.settings.enabled_models}

    def _build_backtest_config(self, frequency: str, train_frame: pd.DataFrame) -> BacktestConfig | None:
        horizon = self.settings.horizon_for(frequency)
        minimum_initial = min(self.settings.initial_window_for(frequency), max(len(train_frame) - horizon, 0))
        if len(train_frame) < horizon + max(10, minimum_initial):
            return None
        return BacktestConfig(
            frequency=frequency,
            horizon=horizon,
            initial_window=minimum_initial,
            step=self.settings.step_for(frequency),
            max_folds=self.settings.max_backtest_folds,
        )

    def _build_station_profiles(self, aggregate_frame: pd.DataFrame, frequency: str) -> pd.DataFrame:
        stations = aggregate_frame.loc[aggregate_frame["segment_type"] == "start_station"].copy()
        if stations.empty:
            return pd.DataFrame()

        grouped = (
            stations.groupby("segment_id", as_index=False)
            .agg(
                row_count=("bucket_start", "count"),
                observed_rows=("is_observed", "sum"),
                total_trip_count=("trip_count", "sum"),
                average_trip_count=("trip_count", "mean"),
                zero_share=("trip_count", lambda values: float((values == 0).mean())),
            )
            .sort_values("total_trip_count", ascending=False)
            .reset_index(drop=True)
        )
        grouped["segment_id"] = grouped["segment_id"].astype(str)
        grouped["frequency"] = frequency
        grouped["station_id"] = grouped["segment_id"]
        grouped["volume_rank"] = range(1, len(grouped) + 1)

        min_rows = (
            self.settings.initial_window_for(frequency)
            + self.settings.validation_window_for(frequency)
            + self.settings.test_window_for(frequency)
        )
        high_volume_threshold = grouped["total_trip_count"].quantile(0.75)
        grouped["volume_tier"] = grouped["total_trip_count"].map(
            lambda value: "high_volume" if value >= high_volume_threshold else "medium_volume"
        )
        grouped.loc[grouped["row_count"] < min_rows, "volume_tier"] = "sparse"

        direct_enabled = frequency in self.settings.station_level_frequencies and self.settings.station_level_top_n > 0
        grouped["direct_modeling_eligible"] = (
            direct_enabled
            & (grouped["row_count"] >= min_rows)
            & (grouped["volume_rank"] <= self.settings.station_level_top_n)
        )
        grouped["modeling_strategy"] = grouped["direct_modeling_eligible"].map(
            lambda eligible: "direct_model" if eligible else "share_allocation"
        )
        grouped["forecastability_reason"] = grouped.apply(
            lambda row: (
                "Direct station model enabled for a sufficiently long, high-priority series."
                if row["direct_modeling_eligible"]
                else "Station kept in the hierarchy through share allocation because it is outside the direct-model set or too sparse."
            ),
            axis=1,
        )

        recent_window = max(self.settings.validation_window_for(frequency), infer_season_length(frequency))
        recent = stations.sort_values("bucket_start").groupby("segment_id").tail(recent_window)
        recent_totals = recent.groupby("segment_id")["trip_count"].sum().astype(float)
        total_recent = recent_totals.sum()
        grouped["recent_share"] = grouped["segment_id"].map(
            lambda station_id: float(recent_totals.get(station_id, 0.0) / total_recent) if total_recent else 0.0
        )
        return grouped[
            [
                "frequency",
                "station_id",
                "row_count",
                "observed_rows",
                "total_trip_count",
                "average_trip_count",
                "zero_share",
                "volume_rank",
                "volume_tier",
                "direct_modeling_eligible",
                "modeling_strategy",
                "forecastability_reason",
                "recent_share",
            ]
        ]

    def _select_training_segments(
        self,
        aggregate_frame: pd.DataFrame,
        frequency: str,
        station_profiles: pd.DataFrame,
    ) -> list[tuple[str, str]]:
        segments: list[tuple[str, str]] = [("system_total", "all")]
        if not station_profiles.empty and frequency in self.settings.station_level_frequencies:
            direct_stations = station_profiles.loc[station_profiles["direct_modeling_eligible"], "station_id"].astype(str).tolist()
            segments.extend([("start_station", station_id) for station_id in direct_stations])
        return segments

    def _prepare_forecast_frame(
        self,
        forecast_frame: pd.DataFrame,
        pipeline_run_id: str,
        frequency: str,
        segment_type: str,
        segment_id: str | None,
        model_name: str,
        generated_at: pd.Timestamp,
        temporal_split,
        per_row_segment: bool = False,
    ) -> dict[str, object]:
        forecast_frame = forecast_frame.copy().reset_index(drop=True)
        if per_row_segment:
            segment_ids = forecast_frame["segment_id"].astype(str)
            forecast_frame["segment_type"] = segment_type
            forecast_frame["segment_id"] = segment_ids
        else:
            forecast_frame["segment_type"] = segment_type
            forecast_frame["segment_id"] = str(segment_id)
        forecast_frame["forecast_level"] = forecast_frame["segment_type"].map(
            lambda value: "station" if value == "start_station" else "total"
        )
        forecast_frame["forecast_id"] = [
            f"{pipeline_run_id}_{frequency}_{forecast_frame.iloc[idx]['segment_type']}_{forecast_frame.iloc[idx]['segment_id']}_{model_name}_{idx}"
            for idx in range(len(forecast_frame))
        ]
        forecast_frame["pipeline_run_id"] = pipeline_run_id
        forecast_frame["frequency"] = frequency
        forecast_frame["model_name"] = model_name
        forecast_frame["model_family"] = MODEL_FAMILY_MAP.get(model_name, model_name)
        forecast_frame["generated_at"] = generated_at
        forecast_frame["horizon"] = range(1, len(forecast_frame) + 1)
        if temporal_split is not None:
            forecast_frame["training_window_start"] = temporal_split.train_frame["bucket_start"].min()
            forecast_frame["training_window_end"] = temporal_split.train_frame["bucket_start"].max()
            forecast_frame["validation_window_start"] = temporal_split.validation_frame["bucket_start"].min()
            forecast_frame["validation_window_end"] = temporal_split.validation_frame["bucket_start"].max()
            forecast_frame["test_window_start"] = temporal_split.test_frame["bucket_start"].min()
            forecast_frame["test_window_end"] = temporal_split.test_frame["bucket_start"].max()
        else:
            for column in (
                "training_window_start",
                "training_window_end",
                "validation_window_start",
                "validation_window_end",
                "test_window_start",
                "test_window_end",
            ):
                forecast_frame[column] = pd.NaT

        outputs = forecast_frame[
            [
                "forecast_id",
                "pipeline_run_id",
                "model_name",
                "model_family",
                "forecast_level",
                "frequency",
                "segment_type",
                "segment_id",
                "target_timestamp",
                "prediction",
                "generated_at",
                "horizon",
                "training_window_start",
                "training_window_end",
                "validation_window_start",
                "validation_window_end",
                "test_window_start",
                "test_window_end",
            ]
        ].copy()

        interval_frames = []
        for level in INTERVAL_LEVELS:
            interval_frame = forecast_frame[
                ["forecast_id", f"lower_{level}", f"upper_{level}"]
            ].rename(
                columns={
                    f"lower_{level}": "lower_bound",
                    f"upper_{level}": "upper_bound",
                }
            )
            interval_frame["interval_level"] = level
            interval_frames.append(interval_frame[["forecast_id", "interval_level", "lower_bound", "upper_bound"]])

        return {"outputs": outputs, "intervals": interval_frames}

    def _build_reconciled_station_outputs(
        self,
        frequency: str,
        forecast_inputs: dict[str, object],
        pipeline_run_id: str,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        station_profiles = forecast_inputs.get("station_profiles")
        total_forecast = forecast_inputs.get("total_champion_forecast")
        direct_station_forecasts = forecast_inputs.get("direct_station_forecasts", [])
        if (
            not isinstance(station_profiles, pd.DataFrame)
            or station_profiles.empty
            or not isinstance(total_forecast, pd.DataFrame)
            or total_forecast.empty
        ):
            return pd.DataFrame(), pd.DataFrame()

        direct_station_frame = (
            pd.concat(direct_station_forecasts, ignore_index=True)
            if direct_station_forecasts
            else pd.DataFrame(columns=["target_timestamp", "segment_id", "prediction"])
        )
        all_station_ids = station_profiles["station_id"].astype(str).tolist()
        recent_share_map = dict(zip(station_profiles["station_id"].astype(str), station_profiles["recent_share"]))
        direct_station_ids = sorted(direct_station_frame["segment_id"].astype(str).unique().tolist()) if not direct_station_frame.empty else []
        remaining_station_ids = [station_id for station_id in all_station_ids if station_id not in direct_station_ids]
        remaining_share_total = sum(recent_share_map.get(station_id, 0.0) for station_id in remaining_station_ids)
        if remaining_share_total <= 0 and remaining_station_ids:
            equal_share = 1 / len(remaining_station_ids)
            remaining_share_map = {station_id: equal_share for station_id in remaining_station_ids}
        else:
            remaining_share_map = {
                station_id: (recent_share_map.get(station_id, 0.0) / remaining_share_total) if remaining_share_total else 0.0
                for station_id in remaining_station_ids
            }

        reconciliation_rows = []
        station_rows = []
        for total_row in total_forecast.itertuples():
            timestamp = total_row.target_timestamp
            total_point = float(total_row.prediction)
            direct_slice = direct_station_frame.loc[direct_station_frame["target_timestamp"] == timestamp].copy()
            direct_point_sum = float(direct_slice["prediction"].sum()) if not direct_slice.empty else 0.0
            scale_factor = min(1.0, total_point / direct_point_sum) if direct_point_sum > 0 else 1.0

            for direct_row in direct_slice.itertuples():
                station_record = {
                    "target_timestamp": timestamp,
                    "segment_id": str(direct_row.segment_id),
                    "prediction": float(direct_row.prediction) * scale_factor,
                }
                for level in INTERVAL_LEVELS:
                    station_record[f"lower_{level}"] = float(getattr(direct_row, f"lower_{level}")) * scale_factor
                    station_record[f"upper_{level}"] = float(getattr(direct_row, f"upper_{level}")) * scale_factor
                station_rows.append(station_record)

            direct_after_scale = sum(
                row["prediction"]
                for row in station_rows
                if row["target_timestamp"] == timestamp and row["segment_id"] in direct_station_ids
            )
            residual_total = max(total_point - direct_after_scale, 0.0)

            for station_id in remaining_station_ids:
                share = remaining_share_map.get(station_id, 0.0)
                station_record = {
                    "target_timestamp": timestamp,
                    "segment_id": station_id,
                    "prediction": residual_total * share,
                }
                for level in INTERVAL_LEVELS:
                    lower_total = float(getattr(total_row, f"lower_{level}"))
                    upper_total = float(getattr(total_row, f"upper_{level}"))
                    station_record[f"lower_{level}"] = max(lower_total * share, 0.0)
                    station_record[f"upper_{level}"] = max(upper_total * share, 0.0)
                station_rows.append(station_record)

            reconciliation_rows.append(
                {
                    "pipeline_run_id": pipeline_run_id,
                    "frequency": frequency,
                    "target_timestamp": timestamp,
                    "direct_total_prediction": total_point,
                    "direct_station_modeled_sum": direct_point_sum,
                    "reconciled_station_sum": total_point,
                    "unmodeled_station_allocated_sum": residual_total,
                    "reconciliation_scale_factor": scale_factor,
                    "direct_station_count": len(direct_station_ids),
                    "allocated_station_count": len(remaining_station_ids),
                }
            )

        station_forecasts = pd.DataFrame(station_rows)
        if not station_forecasts.empty:
            station_forecasts["model_name"] = "reconciled_station_hybrid"
        return pd.DataFrame(reconciliation_rows), station_forecasts

    def _write_artifacts(self, pipeline_run_id: str, frames: dict[str, pd.DataFrame]) -> dict[str, str]:
        artifact_paths: dict[str, str] = {}
        for name, frame in frames.items():
            if frame.empty:
                continue
            if name in {"forecast_outputs", "forecast_intervals"}:
                output_path = self.settings.outputs_forecasts_dir / f"{pipeline_run_id}_{name}.csv"
            else:
                output_path = self.settings.outputs_reports_dir / f"{pipeline_run_id}_{name}.csv"
            frame.to_csv(output_path, index=False)
            artifact_paths[name] = str(output_path)
        return artifact_paths

    def _persist_pipeline_log(
        self,
        pipeline_run_id: str,
        run_started_at: pd.Timestamp,
        status: str,
        message: str,
        records_ingested: int | None = None,
        records_cleaned: int | None = None,
        min_event_timestamp=None,
        max_event_timestamp=None,
        metadata_json: dict[str, object] | None = None,
    ) -> None:
        self._persist_if_configured(
            "pipeline_run_log",
            pd.DataFrame(
                [
                    {
                        "pipeline_run_id": pipeline_run_id,
                        "run_started_at": run_started_at,
                        "run_finished_at": pd.Timestamp.utcnow() if status == "completed" else None,
                        "status": status,
                        "run_type": "full_pipeline",
                        "records_ingested": records_ingested,
                        "records_cleaned": records_cleaned,
                        "min_event_timestamp": min_event_timestamp,
                        "max_event_timestamp": max_event_timestamp,
                        "message": message,
                        "metadata_json": metadata_json or {},
                    }
                ]
            ),
            ["pipeline_run_id"],
        )

    @staticmethod
    def _prepare_monitoring_rows(frame: pd.DataFrame) -> pd.DataFrame:
        if frame.empty:
            return frame
        prepared = frame.copy()
        prepared["monitoring_id"] = [
            f"{row.pipeline_run_id}_{row.check_name if hasattr(row, 'check_name') else row.metric_name}_{idx}"
            for idx, row in enumerate(prepared.itertuples())
        ]
        prepared["created_at"] = pd.Timestamp.utcnow()
        return prepared
