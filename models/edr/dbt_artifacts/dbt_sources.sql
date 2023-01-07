{{
  config(
    materialized='incremental',
    transient=False,
    post_hook='{{ elementary.upload_dbt_sources() }}'
    )
}}

{{ elementary.get_dbt_sources_empty_table_query() }}