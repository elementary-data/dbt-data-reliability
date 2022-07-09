{{
  config(
    materialized='incremental',
    unique_key = 'column_state_id'
  )
}}

{{ elementary.empty_schema_columns_snapshot() }}