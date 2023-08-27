{% test volume_anomalies(
    model,
    timestamp_column=none,
    where_expression=none,
    anomaly_sensitivity=none,
    anomaly_direction="drop",
    min_training_set_size=none,
    time_bucket=none,
    days_back=none,
    backfill_days=none,
    seasonality=none,
    sensitivity=none
) %}
    -- depends_on: {{ ref('monitors_runs') }}
    -- depends_on: {{ ref('data_monitoring_metrics') }}
    -- depends_on: {{ ref('dbt_run_results') }}
    {{
        elementary.test_table_anomalies(
            model=model,
            table_anomalies=["row_count"],
            freshness_column=none,
            timestamp_column=timestamp_column,
            where_expression=where_expression,
            anomaly_sensitivity=anomaly_sensitivity,
            anomaly_direction=anomaly_direction,
            min_training_set_size=min_training_set_size,
            time_bucket=time_bucket,
            days_back=days_back,
            backfill_days=backfill_days,
            seasonality=seasonality,
            sensitivity=sensitivity,
        )
    }}
{% endtest %}
