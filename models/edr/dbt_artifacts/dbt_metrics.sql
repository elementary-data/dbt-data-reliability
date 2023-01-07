{{
  config(
    materialized='incremental',
    transient=False,
    post_hook='{{ elementary.upload_dbt_metrics() }}'
    )
}}

{{ elementary.get_dbt_metrics_empty_table_query() }}