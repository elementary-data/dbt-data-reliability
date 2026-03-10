{{
    config(
        materialized="incremental",
        transient=False,
        unique_key="invocation_id",
        on_schema_change="append_new_columns",
        partition_by=(
            {"field": "created_at", "data_type": "timestamp", "granularity": "day"}
            if target.type == "bigquery"
            and not elementary.get_config_var("bigquery_disable_partitioning")
            else none
        ),
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
