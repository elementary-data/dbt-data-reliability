{{
  config(
    materialized='incremental',
    unique_key = 'id'
  )
}}

-- depends_on: {{ ref('final_tables_config') }}

{{ elementary.empty_data_monitoring_metrics() }}