{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id',
    on_schema_change = 'append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={
      "timestamp_column": "created_at",
      "prev_timestamp_column": "detected_at",
      }
  )
}}

{{ elementary.empty_schema_columns_snapshot() }}