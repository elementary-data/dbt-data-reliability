{{
  config(
    materialized='table',
    transient=False,
    post_hook='{{ elementary.upload_dbt_exposures() }}'
    )
}}

{{ elementary.get_dbt_exposures_empty_table_query() }}