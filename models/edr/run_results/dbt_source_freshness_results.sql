{{
    config(
        materialized="incremental",
        unique_key="source_freshness_execution_id",
        on_schema_change="append_new_columns",
        full_refresh=elementary.get_config_var("elementary_full_refresh"),
        meta={
            "timestamp_column": "created_at",
            "prev_timestamp_column": "generated_at",
        },
        table_type=elementary.get_default_table_type(),
        incremental_strategy=elementary.get_default_incremental_strategy(),
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
