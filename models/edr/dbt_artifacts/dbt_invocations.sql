{{
    config(
        materialized="incremental",
        transient=False,
        unique_key="invocation_id",
        on_schema_change="append_new_columns",
        partition_by=elementary.get_partition_by(),
        full_refresh=elementary.get_config_var("elementary_full_refresh"),
        meta={
            "timestamp_column": "created_at",
            "prev_timestamp_column": "generated_at",
        },
        table_type=elementary.get_default_table_type(),
        incremental_strategy=elementary.get_default_incremental_strategy(),
    )
}}

{{ elementary.get_dbt_invocations_empty_table_query() }}
