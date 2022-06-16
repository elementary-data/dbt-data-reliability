{{
  config(
    materialized='table',
    post_hook=after_commit('{{ elementary.upload_dbt_tests() }}')
  )
}}

{{ elementary.get_dbt_tests_empty_table_query() }}