{#
    Test: volume_threshold
    
    Monitors row count changes using percentage thresholds with multiple severity levels.
    Uses Elementary's metric caching infrastructure to avoid recalculating row counts
    for buckets that have already been computed.
    
    Parameters:
        timestamp_column (required): Column to determine time periods
        warn_threshold_percent (optional): % change that triggers warning (default: 5)
        error_threshold_percent (optional): % change that triggers error (default: 10)
        direction (optional): 'both', 'spike', or 'drop' (default: 'both')
        time_bucket (optional): Time bucket config, e.g. {period: 'day', count: 1}
        where_expression (optional): Additional WHERE filter
        days_back (optional): Days of history to keep (default: 14)
        backfill_days (optional): Days to backfill on each run (default: 2)
        min_row_count (optional): Min baseline rows in previous bucket to trigger check (default: 100)
    
    Example:
        - elementary.volume_threshold:
            timestamp_column: created_at
            warn_threshold_percent: 5
            error_threshold_percent: 10
            direction: both
#}
{% test volume_threshold(
    model,
    timestamp_column,
    warn_threshold_percent=5,
    error_threshold_percent=10,
    direction="both",
    time_bucket=none,
    where_expression=none,
    days_back=14,
    backfill_days=2,
    min_row_count=100
) %}
    {{
        config(
            tags=["elementary-tests"],
            fail_calc="max(severity_level)",
            warn_if=">=1",
            error_if=">=2",
        )
    }}

    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}

        {% if warn_threshold_percent < 0 or error_threshold_percent < 0 %}
            {{
                exceptions.raise_compiler_error(
                    "warn_threshold_percent and error_threshold_percent must be non-negative"
                )
            }}
        {% endif %}
        {% if min_row_count < 0 %}
            {{ exceptions.raise_compiler_error("min_row_count must be non-negative") }}
        {% endif %}
        {% if warn_threshold_percent > error_threshold_percent %}
            {{
                exceptions.raise_compiler_error(
                    "warn_threshold_percent cannot exceed error_threshold_percent"
                )
            }}
        {% endif %}
        {% if direction not in ["both", "spike", "drop"] %}
            {{
                exceptions.raise_compiler_error(
                    "direction must be 'both', 'spike', or 'drop'"
                )
            }}
        {% endif %}

        {% set model_relation = elementary.get_model_relation_for_test(
            model, elementary.get_test_model()
        ) %}
        {% if not model_relation %}
            {{ exceptions.raise_compiler_error("Unsupported model: " ~ model) }}
        {% endif %}

        {%- if elementary.is_ephemeral_model(model_relation) %}
            {{
                exceptions.raise_compiler_error(
                    "Test not supported for ephemeral models: "
                    ~ model_relation.identifier
                )
            }}
        {%- endif %}

        {# Validate timestamp column exists and is a timestamp type #}
        {% set timestamp_column_data_type = (
            elementary.find_normalized_data_type_for_column(
                model_relation, timestamp_column
            )
        ) %}
        {% if not elementary.is_column_timestamp(
            model_relation, timestamp_column, timestamp_column_data_type
        ) %}
            {{
                exceptions.raise_compiler_error(
                    "Column '"
                    ~ timestamp_column
                    ~ "' is not a timestamp type. The timestamp_column must be a timestamp or datetime column."
                )
            }}
        {% endif %}

        {# Collect row_count metrics using Elementary's shared infrastructure.
           This handles: incremental bucket detection, metric computation, temp table creation, cache storage.
           Pass time_bucket as-is (none = use model/project default via get_time_bucket). #}
        {% set table_metrics = [{"type": "row_count", "name": "row_count"}] %}
        {% do elementary.collect_table_metrics(
            table_metrics=table_metrics,
            model_expr=model,
            model_relation=model_relation,
            timestamp_column=timestamp_column,
            time_bucket=time_bucket,
            days_back=days_back,
            backfill_days=backfill_days,
            where_expression=where_expression,
            dimensions=[],
        ) %}

        {# Build metric_properties to match the filter used by collect_table_metrics.
           This must produce the same dict so our data_monitoring_metrics query matches. #}
        {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
        {% set metric_props = elementary.get_metric_properties(
            model_graph_node,
            timestamp_column,
            where_expression,
            time_bucket,
            [],
        ) %}

        {# Compare current vs previous bucket, combining cached history + newly computed metrics #}
        {{
            elementary.get_volume_threshold_comparison_query(
                model_relation=model_relation,
                metric_props=metric_props,
                warn_threshold_percent=warn_threshold_percent,
                error_threshold_percent=error_threshold_percent,
                direction=direction,
                min_row_count=min_row_count,
            )
        }}

    {%- else %} {{ elementary.no_results_query() }}
    {%- endif %}
{% endtest %}


{% macro get_volume_threshold_comparison_query(
    model_relation,
    metric_props,
    warn_threshold_percent,
    error_threshold_percent,
    direction,
    min_row_count
) %}

    {% set data_monitoring_metrics_table = elementary.get_elementary_relation(
        "data_monitoring_metrics"
    ) %}
    {% set test_metrics_table = elementary.get_elementary_test_table(
        elementary.get_elementary_test_table_name(), "metrics"
    ) %}
    {% set full_table_name = elementary.relation_to_full_name(model_relation) %}

    with

        {# Union persisted history with newly computed metrics from this run #}
        all_metrics as (
            select
                bucket_start,
                bucket_end,
                metric_value as row_count,
                1 as source_priority
            from {{ data_monitoring_metrics_table }}
            where
                upper(full_table_name) = upper('{{ full_table_name }}')
                and lower(metric_name) = 'row_count'
                and metric_properties
                = {{ elementary.dict_to_quoted_json(metric_props) }}

            union all

            select
                bucket_start,
                bucket_end,
                metric_value as row_count,
                0 as source_priority
            from {{ test_metrics_table }}
        ),

        {# Deduplicate: prefer freshly computed metrics (priority 0) over cached history (priority 1) #}
        ranked_metrics as (
            select
                bucket_start,
                bucket_end,
                row_count,
                row_number() over (
                    partition by bucket_start, bucket_end order by source_priority asc
                ) as rn
            from all_metrics
        ),

        metrics as (
            select bucket_start, bucket_end, row_count from ranked_metrics where rn = 1
        ),

        {# Use ROW_NUMBER + self-join instead of LAG to avoid a DuckDB internal
           binder bug where LAG over UNION ALL sources confuses TIMESTAMP and
           FLOAT column types (Failed to bind column reference "bucket_end").
           Cast bucket_num to signed int to avoid ClickHouse UInt64/Int64 type
           mismatch in the JOIN condition (bucket_num - 1 promotes to Int64).
           Use a separate max_bucket CTE instead of a scalar subquery to avoid
           SQL Server / Fabric "Invalid object name" errors when referencing a
           CTE inside a nested subquery in another CTE's WHERE clause. #}
        bucket_numbered as (
            select
                bucket_start,
                bucket_end,
                row_count,
                {{
                    elementary.edr_cast_as_int(
                        "row_number() over (order by bucket_end)"
                    )
                }} as bucket_num
            from metrics
        ),

        max_bucket as (select max(bucket_num) as max_num from bucket_numbered),

        comparison as (
            select
                curr.bucket_end as current_period,
                prev_b.bucket_end as previous_period,
                {{ elementary.edr_cast_as_int("curr.row_count") }} as current_row_count,
                {{ elementary.edr_cast_as_int("prev_b.row_count") }}
                as previous_row_count,
                case
                    when prev_b.row_count is null
                    then null
                    when prev_b.row_count = 0
                    then case when curr.row_count > 0 then 999999.99 else 0 end
                    else
                        round(
                            cast(
                                (curr.row_count - prev_b.row_count)
                                * 100.0
                                / prev_b.row_count
                                as {{ elementary.edr_type_numeric() }}
                            ),
                            2
                        )
                end as percent_change
            from bucket_numbered curr
            inner join max_bucket on curr.bucket_num = max_bucket.max_num
            left join bucket_numbered prev_b on prev_b.bucket_num = curr.bucket_num - 1
        ),

        volume_result as (
            select
                *,
                case
                    when
                        previous_row_count is null
                        or previous_row_count < {{ min_row_count }}
                    then 0
                    when percent_change is null
                    then 0
                    {% if direction == "both" %}
                        when abs(percent_change) >= {{ error_threshold_percent }}
                        then 2
                        when abs(percent_change) >= {{ warn_threshold_percent }}
                        then 1
                    {% elif direction == "spike" %}
                        when percent_change >= {{ error_threshold_percent }}
                        then 2
                        when percent_change >= {{ warn_threshold_percent }}
                        then 1
                    {% else %}
                        when percent_change <= -{{ error_threshold_percent }}
                        then 2
                        when percent_change <= -{{ warn_threshold_percent }}
                        then 1
                    {% endif %}
                    else 0
                end as severity_level
            from comparison
        )

    select
        '{{ model_relation.identifier }}' as model_name,
        cast(current_period as {{ elementary.edr_type_string() }}) as current_period,
        cast(previous_period as {{ elementary.edr_type_string() }}) as previous_period,
        current_row_count,
        previous_row_count,
        current_row_count - previous_row_count as absolute_change,
        percent_change,
        severity_level,
        case
            severity_level when 2 then 'error' when 1 then 'warn' else 'pass'
        end as severity_name,
        {{
            elementary.edr_concat(
                [
                    "'Row count changed by '",
                    "cast(percent_change as " ~ elementary.edr_type_string() ~ ")",
                    "'% (from '",
                    "cast(previous_row_count as "
                    ~ elementary.edr_type_string()
                    ~ ")",
                    "' to '",
                    "cast(current_row_count as " ~ elementary.edr_type_string() ~ ")",
                    "')'",
                ]
            )
        }} as result_description
    from volume_result

{% endmacro %}
