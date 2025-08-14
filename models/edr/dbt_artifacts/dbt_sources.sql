{{
  config(
    materialized='incremental',
    transient=False,
    post_hook='{{ elementary.upload_dbt_sources() }}',
    unique_key='unique_id',
    on_schema_change='sync_all_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    incremental_strategy=elementary.get_default_incremental_strategy(),
    meta = {
      "table_type": elementary.get_default_table_type(),
    }
    )
}}

{{ elementary.get_dbt_sources_empty_table_query() }}
