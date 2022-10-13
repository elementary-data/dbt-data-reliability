{{
  config(
    materialized='table',
    transient=False
  )
}}

{{ elementary.get_dbt_models_empty_table_query() }}