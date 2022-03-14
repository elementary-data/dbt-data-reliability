{{
  config(
    materialized='incremental',
    unique_key = 'id'
  )
}}

{{ elementary.empty_data_monitoring_metrics() }}