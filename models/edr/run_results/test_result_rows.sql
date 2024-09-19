-- indexes are not supported in all warehouses, relevant to postgres only
{{
  config(
    materialized = 'incremental',
    unique_key = 'elementary_test_results_id',
    on_schema_change = 'append_new_columns',
    indexes=[{'columns': ['created_at']}, {'columns': ['elementary_test_results_id']}] if target.type == "postgres" else [],
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={
      "timestamp_column": "created_at",
      "prev_timestamp_column": "detected_at",
      },
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}

-- depends_on: {{ ref('elementary_test_results') }}
{{ elementary.empty_table([
    ('elementary_test_results_id', 'long_string'),
    ('result_row', 'long_string'),
    ('detected_at','timestamp'),
    ('created_at','timestamp'),
]) }}
