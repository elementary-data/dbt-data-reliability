{{
  config(
    materialized='incremental',
    transient=False,
    post_hook=after_commit('{{ elementary.upload_dbt_exposures() }}')
    )
}}

{{ elementary.get_dbt_exposures_empty_table_query() }}