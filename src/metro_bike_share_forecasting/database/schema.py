from __future__ import annotations

try:
    from sqlalchemy import (
        JSON,
        Boolean,
        Column,
        DateTime,
        Float,
        Integer,
        MetaData,
        String,
        Table,
        Text,
    )
except ModuleNotFoundError:  # pragma: no cover - optional until dependencies are installed
    JSON = Boolean = Column = DateTime = Float = Integer = MetaData = String = Table = Text = None


def build_metadata(schema_name: str) -> MetaData:
    if MetaData is None:  # pragma: no cover - dependency guard
        raise ModuleNotFoundError("sqlalchemy is required to build PostgreSQL metadata.")
    metadata = MetaData(schema=schema_name)

    Table(
        "pipeline_run_log",
        metadata,
        Column("pipeline_run_id", String, primary_key=True),
        Column("run_started_at", DateTime(timezone=True), nullable=False),
        Column("run_finished_at", DateTime(timezone=True)),
        Column("status", String, nullable=False),
        Column("run_type", String, nullable=False),
        Column("records_ingested", Integer),
        Column("records_cleaned", Integer),
        Column("min_event_timestamp", DateTime(timezone=True)),
        Column("max_event_timestamp", DateTime(timezone=True)),
        Column("message", Text),
        Column("metadata_json", JSON),
    )

    Table(
        "raw_ingestion_log",
        metadata,
        Column("pipeline_run_id", String, primary_key=True),
        Column("source_file", String, primary_key=True),
        Column("ingestion_timestamp", DateTime(timezone=True), nullable=False),
        Column("record_count_raw", Integer),
        Column("record_count_cleaned", Integer),
        Column("min_event_timestamp", Text),
        Column("max_event_timestamp", Text),
        Column("duplicates_removed", Integer),
        Column("schema_drift_detected", Boolean),
        Column("schema_columns", JSON),
    )

    Table(
        "cleaned_trip_data",
        metadata,
        Column("trip_id", String, primary_key=True),
        Column("source_file", String),
        Column("source_row_number", Integer),
        Column("pipeline_run_id", String, nullable=False),
        Column("duration_minutes", Float),
        Column("start_ts_utc", DateTime(timezone=True)),
        Column("end_ts_utc", DateTime(timezone=True)),
        Column("start_ts_local", DateTime),
        Column("end_ts_local", DateTime),
        Column("start_station", Integer),
        Column("end_station", Integer),
        Column("start_lat", Float),
        Column("start_lon", Float),
        Column("end_lat", Float),
        Column("end_lon", Float),
        Column("bike_id", Integer),
        Column("plan_duration", Integer),
        Column("trip_route_category", String),
        Column("passholder_type", String),
        Column("bike_type", String),
        Column("event_date_local", String),
        Column("event_hour_local", Integer),
        Column("raw_record_json", JSON),
    )

    for table_name in (
        "aggregated_hourly",
        "aggregated_daily",
        "aggregated_weekly",
        "aggregated_monthly",
        "aggregated_quarterly",
    ):
        Table(
            table_name,
            metadata,
            Column("bucket_start", DateTime, primary_key=True),
            Column("segment_type", String, primary_key=True),
            Column("segment_id", String, primary_key=True),
            Column("bucket_end", DateTime),
            Column("trip_count", Integer),
            Column("distinct_bikes", Integer),
            Column("avg_duration_minutes", Float),
            Column("is_observed", Boolean),
            Column("missing_period_flag", Boolean),
            Column("pipeline_run_id", String),
            Column("generated_at", DateTime(timezone=True)),
        )

    for table_name in (
        "feature_store_hourly",
        "feature_store_daily",
        "feature_store_weekly",
        "feature_store_monthly",
    ):
        Table(
            table_name,
            metadata,
            Column("bucket_start", DateTime, primary_key=True),
            Column("segment_type", String, primary_key=True),
            Column("segment_id", String, primary_key=True),
            Column("target_trip_count", Float),
            Column("pandemic_phase", String),
            Column("completeness_flag", Boolean),
            Column("pipeline_run_id", String),
            Column("generated_at", DateTime(timezone=True)),
            Column("feature_payload", JSON),
        )

    Table(
        "model_registry",
        metadata,
        Column("model_id", String, primary_key=True),
        Column("model_name", String, nullable=False),
        Column("model_family", String, nullable=False),
        Column("frequency", String, nullable=False),
        Column("forecast_level", String, nullable=False),
        Column("horizon", Integer, nullable=False),
        Column("segment_type", String, nullable=False),
        Column("segment_id", String, nullable=False),
        Column("version", String, nullable=False),
        Column("training_window_start", DateTime),
        Column("training_window_end", DateTime),
        Column("validation_window_start", DateTime),
        Column("validation_window_end", DateTime),
        Column("test_window_start", DateTime),
        Column("test_window_end", DateTime),
        Column("trained_at", DateTime(timezone=True)),
        Column("pipeline_run_id", String),
        Column("parameters_json", JSON),
    )

    Table(
        "backtest_results",
        metadata,
        Column("result_id", String, primary_key=True),
        Column("pipeline_run_id", String, nullable=False),
        Column("model_name", String, nullable=False),
        Column("frequency", String, nullable=False),
        Column("window_role", String, nullable=False),
        Column("metric_scope", String, nullable=False),
        Column("horizon_bucket", String),
        Column("segment_type", String, nullable=False),
        Column("segment_id", String, nullable=False),
        Column("fold_id", Integer),
        Column("evaluation_regime", String),
        Column("holdout_rows", Integer),
        Column("training_window_start", DateTime),
        Column("training_window_end", DateTime),
        Column("holdout_window_start", DateTime),
        Column("holdout_window_end", DateTime),
        Column("mae", Float),
        Column("rmse", Float),
        Column("mape", Float),
        Column("smape", Float),
        Column("mase", Float),
        Column("pinball_50", Float),
        Column("pinball_80", Float),
        Column("pinball_95", Float),
        Column("coverage_50", Float),
        Column("coverage_80", Float),
        Column("coverage_95", Float),
        Column("width_50", Float),
        Column("width_80", Float),
        Column("width_95", Float),
        Column("bias", Float),
    )

    Table(
        "evaluation_predictions",
        metadata,
        Column("prediction_id", String, primary_key=True),
        Column("pipeline_run_id", String, nullable=False),
        Column("model_name", String, nullable=False),
        Column("frequency", String, nullable=False),
        Column("window_role", String, nullable=False),
        Column("fold_id", Integer),
        Column("segment_type", String, nullable=False),
        Column("segment_id", String, nullable=False),
        Column("target_timestamp", DateTime, nullable=False),
        Column("horizon_step", Integer),
        Column("evaluation_regime", String),
        Column("prediction", Float),
        Column("actual", Float),
        Column("lower_50", Float),
        Column("upper_50", Float),
        Column("lower_80", Float),
        Column("upper_80", Float),
        Column("lower_95", Float),
        Column("upper_95", Float),
        Column("training_window_start", DateTime),
        Column("training_window_end", DateTime),
        Column("holdout_window_start", DateTime),
        Column("holdout_window_end", DateTime),
    )

    Table(
        "split_metadata",
        metadata,
        Column("pipeline_run_id", String, primary_key=True),
        Column("frequency", String, primary_key=True),
        Column("segment_type", String, primary_key=True),
        Column("segment_id", String, primary_key=True),
        Column("window_role", String, primary_key=True),
        Column("window_start", DateTime),
        Column("window_end", DateTime),
        Column("row_count", Integer),
    )

    Table(
        "station_modeling_registry",
        metadata,
        Column("pipeline_run_id", String, primary_key=True),
        Column("frequency", String, primary_key=True),
        Column("station_id", String, primary_key=True),
        Column("row_count", Integer),
        Column("observed_rows", Integer),
        Column("total_trip_count", Float),
        Column("average_trip_count", Float),
        Column("zero_share", Float),
        Column("volume_rank", Integer),
        Column("volume_tier", String),
        Column("direct_modeling_eligible", Boolean),
        Column("modeling_strategy", String),
        Column("forecastability_reason", Text),
        Column("recent_share", Float),
    )

    Table(
        "forecast_outputs",
        metadata,
        Column("forecast_id", String, primary_key=True),
        Column("pipeline_run_id", String, nullable=False),
        Column("model_name", String, nullable=False),
        Column("model_family", String, nullable=False),
        Column("forecast_level", String, nullable=False),
        Column("frequency", String, nullable=False),
        Column("segment_type", String, nullable=False),
        Column("segment_id", String, nullable=False),
        Column("target_timestamp", DateTime, nullable=False),
        Column("prediction", Float, nullable=False),
        Column("generated_at", DateTime(timezone=True), nullable=False),
        Column("horizon", Integer, nullable=False),
        Column("training_window_start", DateTime),
        Column("training_window_end", DateTime),
        Column("validation_window_start", DateTime),
        Column("validation_window_end", DateTime),
        Column("test_window_start", DateTime),
        Column("test_window_end", DateTime),
    )

    Table(
        "forecast_intervals",
        metadata,
        Column("forecast_id", String, primary_key=True),
        Column("interval_level", Integer, primary_key=True),
        Column("lower_bound", Float),
        Column("upper_bound", Float),
    )

    Table(
        "drift_monitoring",
        metadata,
        Column("monitoring_id", String, primary_key=True),
        Column("pipeline_run_id", String, nullable=False),
        Column("frequency", String, nullable=False),
        Column("metric_name", String, nullable=False),
        Column("metric_value", Float),
        Column("details", JSON),
        Column("created_at", DateTime(timezone=True)),
    )

    Table(
        "data_quality_monitoring",
        metadata,
        Column("monitoring_id", String, primary_key=True),
        Column("pipeline_run_id", String, nullable=False),
        Column("check_name", String, nullable=False),
        Column("check_value", Float),
        Column("status", String, nullable=False),
        Column("details", JSON),
        Column("created_at", DateTime(timezone=True)),
    )

    Table(
        "champion_model_registry",
        metadata,
        Column("frequency", String, primary_key=True),
        Column("segment_type", String, primary_key=True),
        Column("segment_id", String, primary_key=True),
        Column("model_name", String, nullable=False),
        Column("pipeline_run_id", String, nullable=False),
        Column("selection_reason", Text),
        Column("composite_score", Float),
        Column("updated_at", DateTime(timezone=True)),
        Column("metadata_json", JSON),
    )

    Table(
        "reconciliation_outputs",
        metadata,
        Column("pipeline_run_id", String, primary_key=True),
        Column("frequency", String, primary_key=True),
        Column("target_timestamp", DateTime, primary_key=True),
        Column("direct_total_prediction", Float),
        Column("direct_station_modeled_sum", Float),
        Column("reconciled_station_sum", Float),
        Column("unmodeled_station_allocated_sum", Float),
        Column("reconciliation_scale_factor", Float),
        Column("direct_station_count", Integer),
        Column("allocated_station_count", Integer),
    )

    return metadata
