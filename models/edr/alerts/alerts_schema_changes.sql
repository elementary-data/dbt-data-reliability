{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id'
  )
}}

{{ elementary.empty_alerts() }}