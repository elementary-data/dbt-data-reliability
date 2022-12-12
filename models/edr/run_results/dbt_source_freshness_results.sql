{{
  config(
    materialized = 'incremental',
    unique_key = 'source_freshness_execution_id',
    on_schema_change = 'append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh')
  )
}}

{{ elementary.empty_dbt_source_freshness_results() }}
