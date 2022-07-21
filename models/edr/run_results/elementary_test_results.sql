{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'append_new_columns'
  )
}}

{{ elementary.empty_elementary_test_results() }}