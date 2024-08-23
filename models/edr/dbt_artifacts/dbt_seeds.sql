{{
  config(
    materialized='incremental',
    transient=False,
    post_hook='{{ elementary.upload_dbt_seeds() }}',
    unique_key='unique_id',
    on_schema_change='sync_all_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    table_type=elementary.get_default_table_type(),
    file_format=elementary.get_default_file_format(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}

{{ elementary.get_dbt_seeds_empty_table_query() }}
