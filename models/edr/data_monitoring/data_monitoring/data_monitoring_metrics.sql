{{
  config(
    materialized='incremental',
    unique_key='id',
    on_schema_change='append_new_columns'
  )
}}

{{ elementary.empty_data_monitoring_metrics() }}