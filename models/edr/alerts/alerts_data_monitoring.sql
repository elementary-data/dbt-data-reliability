{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    on_schema_change = 'append_new_columns'
  )
}}

-- depends_on: {{ ref('metrics_anomaly_score') }}

{{ elementary.empty_alerts() }}