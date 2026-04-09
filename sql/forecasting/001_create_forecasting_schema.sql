CREATE SCHEMA IF NOT EXISTS forecasting;

CREATE TABLE IF NOT EXISTS forecasting.pipeline_run_log (
    pipeline_run_id TEXT PRIMARY KEY,
    run_started_at TIMESTAMPTZ NOT NULL,
    run_finished_at TIMESTAMPTZ,
    status TEXT NOT NULL,
    run_type TEXT NOT NULL,
    records_ingested INTEGER,
    records_cleaned INTEGER,
    min_event_timestamp TIMESTAMPTZ,
    max_event_timestamp TIMESTAMPTZ,
    message TEXT,
    metadata_json JSONB
);

CREATE TABLE IF NOT EXISTS forecasting.raw_ingestion_log (
    pipeline_run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    ingestion_timestamp TIMESTAMPTZ NOT NULL,
    record_count_raw INTEGER,
    record_count_cleaned INTEGER,
    min_event_timestamp TEXT,
    max_event_timestamp TEXT,
    duplicates_removed INTEGER,
    schema_drift_detected BOOLEAN,
    schema_columns JSONB,
    PRIMARY KEY (pipeline_run_id, source_file)
);

CREATE TABLE IF NOT EXISTS forecasting.cleaned_trip_data (
    trip_id TEXT PRIMARY KEY,
    source_file TEXT,
    source_row_number INTEGER,
    pipeline_run_id TEXT NOT NULL,
    duration_minutes DOUBLE PRECISION,
    start_ts_utc TIMESTAMPTZ,
    end_ts_utc TIMESTAMPTZ,
    start_ts_local TIMESTAMP,
    end_ts_local TIMESTAMP,
    start_station INTEGER,
    end_station INTEGER,
    start_lat DOUBLE PRECISION,
    start_lon DOUBLE PRECISION,
    end_lat DOUBLE PRECISION,
    end_lon DOUBLE PRECISION,
    bike_id INTEGER,
    plan_duration INTEGER,
    trip_route_category TEXT,
    passholder_type TEXT,
    bike_type TEXT,
    event_date_local TEXT,
    event_hour_local INTEGER,
    raw_record_json JSONB
);

CREATE TABLE IF NOT EXISTS forecasting.aggregated_hourly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    bucket_end TIMESTAMP,
    trip_count INTEGER,
    distinct_bikes INTEGER,
    avg_duration_minutes DOUBLE PRECISION,
    is_observed BOOLEAN,
    missing_period_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.aggregated_daily (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    bucket_end TIMESTAMP,
    trip_count INTEGER,
    distinct_bikes INTEGER,
    avg_duration_minutes DOUBLE PRECISION,
    is_observed BOOLEAN,
    missing_period_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.aggregated_weekly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    bucket_end TIMESTAMP,
    trip_count INTEGER,
    distinct_bikes INTEGER,
    avg_duration_minutes DOUBLE PRECISION,
    is_observed BOOLEAN,
    missing_period_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.aggregated_monthly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    bucket_end TIMESTAMP,
    trip_count INTEGER,
    distinct_bikes INTEGER,
    avg_duration_minutes DOUBLE PRECISION,
    is_observed BOOLEAN,
    missing_period_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.aggregated_quarterly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    bucket_end TIMESTAMP,
    trip_count INTEGER,
    distinct_bikes INTEGER,
    avg_duration_minutes DOUBLE PRECISION,
    is_observed BOOLEAN,
    missing_period_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.feature_store_hourly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_trip_count DOUBLE PRECISION,
    pandemic_phase TEXT,
    completeness_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    feature_payload JSONB,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.feature_store_daily (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_trip_count DOUBLE PRECISION,
    pandemic_phase TEXT,
    completeness_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    feature_payload JSONB,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.feature_store_weekly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_trip_count DOUBLE PRECISION,
    pandemic_phase TEXT,
    completeness_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    feature_payload JSONB,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.feature_store_monthly (
    bucket_start TIMESTAMP NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_trip_count DOUBLE PRECISION,
    pandemic_phase TEXT,
    completeness_flag BOOLEAN,
    pipeline_run_id TEXT,
    generated_at TIMESTAMPTZ,
    feature_payload JSONB,
    PRIMARY KEY (bucket_start, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.model_registry (
    model_id TEXT PRIMARY KEY,
    model_name TEXT NOT NULL,
    model_family TEXT NOT NULL,
    frequency TEXT NOT NULL,
    forecast_level TEXT NOT NULL,
    horizon INTEGER NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    version TEXT NOT NULL,
    training_window_start TIMESTAMP,
    training_window_end TIMESTAMP,
    selection_train_window_start TIMESTAMP,
    selection_train_window_end TIMESTAMP,
    validation_window_start TIMESTAMP,
    validation_window_end TIMESTAMP,
    test_window_start TIMESTAMP,
    test_window_end TIMESTAMP,
    trained_at TIMESTAMPTZ,
    pipeline_run_id TEXT,
    parameters_json JSONB
);

CREATE TABLE IF NOT EXISTS forecasting.backtest_results (
    result_id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    frequency TEXT NOT NULL,
    window_role TEXT NOT NULL,
    metric_scope TEXT NOT NULL,
    horizon_bucket TEXT,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    fold_id INTEGER,
    evaluation_regime TEXT,
    holdout_rows INTEGER,
    training_window_start TIMESTAMP,
    training_window_end TIMESTAMP,
    holdout_window_start TIMESTAMP,
    holdout_window_end TIMESTAMP,
    mae DOUBLE PRECISION,
    rmse DOUBLE PRECISION,
    mape DOUBLE PRECISION,
    smape DOUBLE PRECISION,
    mase DOUBLE PRECISION,
    pinball_50 DOUBLE PRECISION,
    pinball_80 DOUBLE PRECISION,
    pinball_95 DOUBLE PRECISION,
    coverage_50 DOUBLE PRECISION,
    coverage_80 DOUBLE PRECISION,
    coverage_95 DOUBLE PRECISION,
    width_50 DOUBLE PRECISION,
    width_80 DOUBLE PRECISION,
    width_95 DOUBLE PRECISION,
    bias DOUBLE PRECISION
);

CREATE TABLE IF NOT EXISTS forecasting.evaluation_predictions (
    prediction_id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    frequency TEXT NOT NULL,
    window_role TEXT NOT NULL,
    fold_id INTEGER,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_timestamp TIMESTAMP NOT NULL,
    horizon_step INTEGER,
    evaluation_regime TEXT,
    prediction DOUBLE PRECISION,
    actual DOUBLE PRECISION,
    lower_50 DOUBLE PRECISION,
    upper_50 DOUBLE PRECISION,
    lower_80 DOUBLE PRECISION,
    upper_80 DOUBLE PRECISION,
    lower_95 DOUBLE PRECISION,
    upper_95 DOUBLE PRECISION,
    training_window_start TIMESTAMP,
    training_window_end TIMESTAMP,
    holdout_window_start TIMESTAMP,
    holdout_window_end TIMESTAMP
);

CREATE TABLE IF NOT EXISTS forecasting.split_metadata (
    pipeline_run_id TEXT NOT NULL,
    frequency TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    window_role TEXT NOT NULL,
    window_start TIMESTAMP,
    window_end TIMESTAMP,
    row_count INTEGER,
    PRIMARY KEY (pipeline_run_id, frequency, segment_type, segment_id, window_role)
);

CREATE TABLE IF NOT EXISTS forecasting.station_modeling_registry (
    pipeline_run_id TEXT NOT NULL,
    frequency TEXT NOT NULL,
    station_id TEXT NOT NULL,
    row_count INTEGER,
    observed_rows INTEGER,
    total_trip_count DOUBLE PRECISION,
    average_trip_count DOUBLE PRECISION,
    zero_share DOUBLE PRECISION,
    volume_rank INTEGER,
    volume_tier TEXT,
    direct_modeling_eligible BOOLEAN,
    modeling_strategy TEXT,
    forecastability_reason TEXT,
    recent_share DOUBLE PRECISION,
    PRIMARY KEY (pipeline_run_id, frequency, station_id)
);

CREATE TABLE IF NOT EXISTS forecasting.forecast_outputs (
    forecast_id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    model_family TEXT NOT NULL,
    forecast_level TEXT NOT NULL,
    frequency TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    target_timestamp TIMESTAMP NOT NULL,
    prediction DOUBLE PRECISION NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL,
    horizon INTEGER NOT NULL,
    training_window_start TIMESTAMP,
    training_window_end TIMESTAMP,
    selection_train_window_start TIMESTAMP,
    selection_train_window_end TIMESTAMP,
    validation_window_start TIMESTAMP,
    validation_window_end TIMESTAMP,
    test_window_start TIMESTAMP,
    test_window_end TIMESTAMP
);

CREATE TABLE IF NOT EXISTS forecasting.forecast_intervals (
    forecast_id TEXT NOT NULL,
    interval_level INTEGER NOT NULL,
    lower_bound DOUBLE PRECISION,
    upper_bound DOUBLE PRECISION,
    PRIMARY KEY (forecast_id, interval_level)
);

CREATE TABLE IF NOT EXISTS forecasting.drift_monitoring (
    monitoring_id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    frequency TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value DOUBLE PRECISION,
    details JSONB,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS forecasting.data_quality_monitoring (
    monitoring_id TEXT PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    check_name TEXT NOT NULL,
    check_value DOUBLE PRECISION,
    status TEXT NOT NULL,
    details JSONB,
    created_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS forecasting.champion_model_registry (
    frequency TEXT NOT NULL,
    segment_type TEXT NOT NULL,
    segment_id TEXT NOT NULL,
    model_name TEXT NOT NULL,
    pipeline_run_id TEXT NOT NULL,
    selection_reason TEXT,
    composite_score DOUBLE PRECISION,
    updated_at TIMESTAMPTZ,
    metadata_json JSONB,
    PRIMARY KEY (frequency, segment_type, segment_id)
);

CREATE TABLE IF NOT EXISTS forecasting.reconciliation_outputs (
    pipeline_run_id TEXT NOT NULL,
    frequency TEXT NOT NULL,
    target_timestamp TIMESTAMP NOT NULL,
    direct_total_prediction DOUBLE PRECISION,
    direct_station_modeled_sum DOUBLE PRECISION,
    reconciled_station_sum DOUBLE PRECISION,
    unmodeled_station_allocated_sum DOUBLE PRECISION,
    reconciliation_scale_factor DOUBLE PRECISION,
    direct_station_count INTEGER,
    allocated_station_count INTEGER,
    PRIMARY KEY (pipeline_run_id, frequency, target_timestamp)
);
