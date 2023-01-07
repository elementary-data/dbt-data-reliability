{{
  config(
    materialized=elementary.get_dbt_artifacts_materialized(),
    transient=False,
    post_hook='{{ elementary.upload_dbt_models() }}'
  )
}}

{{ elementary.get_dbt_models_empty_table_query() }}