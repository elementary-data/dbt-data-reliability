{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    transient=False,
    post_hook='{{ elementary.upload_dbt_sources() }}',
    unique_key='unique_id',
    on_schema_change='append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh')
    )
}}

{{ elementary.get_dbt_sources_empty_table_query() }}