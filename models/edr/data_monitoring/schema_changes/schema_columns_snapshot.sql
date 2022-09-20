{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id',
    enabled = target.type != 'databricks' and target.type != 'spark' | as_bool()
  )
}}

{{ elementary.empty_schema_columns_snapshot() }}