{{
  config(
    materialized=elementary.get_dbt_artifacts_materialized(),
    transient=False,
    post_hook='{{ elementary.upload_dbt_exposures() }}'
    )
}}

{{ elementary.get_dbt_exposures_empty_table_query() }}