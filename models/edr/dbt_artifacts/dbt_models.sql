{{
  config(
    materialized='table',
    post_hook=after_commit('{{ elementary.upload_dbt_models() }}')
  )
}}

{{ elementary.get_dbt_models_empty_table_query() }}