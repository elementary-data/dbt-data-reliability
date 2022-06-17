{{
  config(
    materialized='table',
    transient=False,
    post_hook='{{ elementary.upload_dbt_tests() }}'
  )
}}

{{ elementary.get_dbt_tests_empty_table_query() }}