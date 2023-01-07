{{
  config(
    materialized='incremental',
    transient=False,
    post_hook=after_commit('{{ elementary.upload_dbt_snapshots() }}')
  )
}}

{{ elementary.get_dbt_models_empty_table_query() }}