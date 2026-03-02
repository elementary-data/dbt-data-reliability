{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns',
    indexes=[{'columns': ['full_table_name', "column_name", "metric_name"]}] if target.type == "postgres" else [],
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={
      "timestamp_column": "created_at",
      "prev_timestamp_column": "updated_at",
      },
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy(),
  )
}}

{{ elementary.empty_data_monitoring_metrics() }}
