{{
  config(
    materialized = 'incremental',
    unique_key = 'alert_id',
    on_schema_change = 'append_new_columns'
  )
}}

{{ elementary.empty_alerts() }}