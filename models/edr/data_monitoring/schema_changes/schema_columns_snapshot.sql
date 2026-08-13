{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns",
        full_refresh=elementary.get_config_var("elementary_full_refresh"),
        meta={
            "dedup_by_column": "column_state_id",
            "timestamp_column": "created_at",
            "prev_timestamp_column": "detected_at",
        },
        table_type=elementary.get_default_table_type(),
        incremental_strategy=elementary.get_append_only_incremental_strategy(),
    )
}}

{{ elementary.empty_schema_columns_snapshot() }}
