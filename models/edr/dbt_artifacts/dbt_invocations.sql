{{
  config(
    materialized = 'incremental',
    transient=False,
    unique_key = 'invocation_id',
    on_schema_change = 'append_new_columns'
  )
}}

{{ elementary.get_dbt_invocations_empty_table_query() }}
