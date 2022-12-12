{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool(),
    full_refresh=elementary.get_config_var('elementary_full_refresh')
  )
}}

{{ elementary.empty_schema_columns_snapshot() }}