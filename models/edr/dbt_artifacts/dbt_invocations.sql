{{
  config(
    materialized = 'incremental',
    transient=False,
    unique_key = 'invocation_id',
    on_schema_change = 'append_new_columns',
    post_hook='{{ elementary.upload_dbt_invocation() }}'
  )
}}

{{ elementary.get_dbt_invocations_empty_table_query() }}
