{{
  config(
    materialized = 'incremental',
    unique_key = 'source_freshness_execution_id',
    on_schema_change = 'append_new_columns',
    full_refresh=var('elementary_full_refresh', false)
  )
}}

{{ elementary.empty_dbt_source_freshness_results() }}
