{{
  config(
    materialized = 'incremental',
    unique_key = 'unique_id',
    on_schema_change = 'append_new_columns'
  )
}}

{{ elementary.empty_source_freshness_results() }}
