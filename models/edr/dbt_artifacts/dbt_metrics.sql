{{
  config(
    materialized='table',
    post_hook=after_commit('{{ elementary.upload_dbt_metrics() }}')
    )
}}

{{ elementary.get_dbt_metrics_empty_table_query() }}