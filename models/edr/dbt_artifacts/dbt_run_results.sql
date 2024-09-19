{{
  config(
    materialized = 'incremental',
    transient=False,
    unique_key = 'model_execution_id',
    on_schema_change = 'append_new_columns',
    indexes=[{'columns': ['unique_id']}] if target.type == "postgres" else [],
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={
      "dedup_by_column": "model_execution_id",
      "timestamp_column": "created_at",
      "prev_timestamp_column": "generated_at",
      },
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy()
  )
}}

{{ elementary.get_dbt_run_results_empty_table_query() }}
