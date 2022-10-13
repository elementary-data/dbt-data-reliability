{{
  config(
    materialized='table',
    transient=False
    )
}}

{{ elementary.get_dbt_sources_empty_table_query() }}