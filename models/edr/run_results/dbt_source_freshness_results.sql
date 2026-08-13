{{
    config(
        materialized="incremental",
        on_schema_change="append_new_columns",
        full_refresh=elementary.get_config_var("elementary_full_refresh"),
        meta={
            "dedup_by_column": "source_freshness_execution_id",
            "timestamp_column": "created_at",
            "prev_timestamp_column": "generated_at",
        },
        table_type=elementary.get_default_table_type(),
        incremental_strategy=elementary.get_append_only_incremental_strategy(),
        indexes=elementary.get_indexes_for_model(
            "dbt_source_freshness_results",
            [
                {"columns": ["unique_id", "created_at"]},
                {"columns": ["source_freshness_execution_id"]},
            ],
        ),
    )
}}

{{ elementary.empty_dbt_source_freshness_results() }}
