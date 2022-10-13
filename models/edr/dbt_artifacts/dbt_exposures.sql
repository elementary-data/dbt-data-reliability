{{
  config(
    materialized='table',
    transient=False
    )
}}

{{ elementary.get_dbt_exposures_empty_table_query() }}