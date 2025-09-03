{{
  config(
    materialized = 'incremental',
    unique_key = 'id',
    on_schema_change = 'append_new_columns',
    full_refresh=elementary.get_config_var('elementary_full_refresh'),
    meta={
      "timestamp_column": "created_at",
      "prev_timestamp_column": "detected_at",
      },
    table_type=elementary.get_default_table_type(),
    incremental_strategy=elementary.get_default_incremental_strategy(),
    indexes=[
      {'columns': ['id']},
      {'columns': ['test_unique_id', 'detected_at']}
    ] if target.type == "postgres" else []
  )
}}

{{ elementary.empty_elementary_test_results() }}
