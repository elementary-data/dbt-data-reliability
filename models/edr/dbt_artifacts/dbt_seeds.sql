{{
  config(
    materialized='incremental',
    transient=False,
    post_hook='{{ elementary.upload_dbt_seeds() }}'
  )
}}

{{ elementary.get_dbt_seeds_empty_table_query() }}