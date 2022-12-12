{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns',
    full_refresh=var('elementary_full_refresh', false)
  )
}}

{{ elementary.empty_data_monitoring_metrics() }}