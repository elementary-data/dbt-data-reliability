{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}

-- depends_on: {{ ref('metrics_anomaly_score') }}

{{ elementary.empty_alerts() }}