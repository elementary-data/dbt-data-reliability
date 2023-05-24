{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id',
    on_schema_change='append_new_columns',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool(),
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={"timestamp_column": "detected_at"},
    post_hook="{{ init_created_at() }}"
  )
}}

{{ elementary.empty_schema_columns_snapshot() }}