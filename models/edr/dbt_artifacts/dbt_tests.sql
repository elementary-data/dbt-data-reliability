{{
  config(
    materialized='table',
    transient=False
  )
}}

{{ elementary.get_dbt_tests_empty_table_query() }}