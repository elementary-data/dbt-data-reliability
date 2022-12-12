{{
  config(
    materialized = 'incremental',
    transient=False,
    unique_key = 'invocation_id',
    on_schema_change = 'append_new_columns',
    full_refresh=var('elementary_full_refresh', false)
  )
}}

{{ elementary.get_dbt_invocations_empty_table_query() }}
