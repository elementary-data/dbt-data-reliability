{{
  config(
    materialized='table',
    transient=False
    )
}}

{{ elementary.get_dbt_metrics_empty_table_query() }}