{#
    SQL Server shares T-SQL with Fabric, so all sqlserver__ dispatches
    delegate to their fabric__ counterparts.
#}
{# ── Data types ─────────────────────────────────────────────────── #}
{% macro sqlserver__edr_type_bool() %}
    {% do return(elementary.fabric__edr_type_bool()) %}
{% endmacro %}
{% macro sqlserver__edr_type_string() %}
    {% do return(elementary.fabric__edr_type_string()) %}
{% endmacro %}
{%- macro sqlserver__edr_type_long_string() -%}
    {% do return(elementary.fabric__edr_type_long_string()) %}
{%- endmacro -%}
{% macro sqlserver__edr_type_timestamp() %}
    {{ elementary.fabric__edr_type_timestamp() }}
{% endmacro %}

{# ── Date / time utilities ──────────────────────────────────────── #}
{% macro sqlserver__edr_current_timestamp() -%}
    {{ elementary.fabric__edr_current_timestamp() }}
{%- endmacro -%}

{% macro sqlserver__edr_current_timestamp_in_utc() -%}
    {{ elementary.fabric__edr_current_timestamp_in_utc() }}
{%- endmacro -%}

{% macro sqlserver__edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    {{ elementary.fabric__edr_dateadd(datepart, interval, from_date_or_timestamp) }}
{% endmacro %}

{% macro sqlserver__edr_datediff(first_date, second_date, date_part) %}
    {{ elementary.fabric__edr_datediff(first_date, second_date, date_part) }}
{% endmacro %}

{% macro sqlserver__edr_timeadd(date_part, number, timestamp_expression) %}
    {{ elementary.fabric__edr_timeadd(date_part, number, timestamp_expression) }}
{% endmacro %}

{% macro sqlserver__edr_date_trunc(datepart, date_expression) %}
    {{ elementary.fabric__edr_date_trunc(datepart, date_expression) }}
{% endmacro %}

{% macro sqlserver__edr_time_trunc(date_part, date_expression) %}
    {{ elementary.fabric__edr_time_trunc(date_part, date_expression) }}
{% endmacro %}

{% macro sqlserver__edr_to_char(column, format) %}
    {{ elementary.fabric__edr_to_char(column, format) }}
{% endmacro %}

{% macro sqlserver__edr_day_of_week_expression(date_expr) %}
    {{ elementary.fabric__edr_day_of_week_expression(date_expr) }}
{% endmacro %}

{% macro sqlserver__edr_hour_of_week_expression(date_expr) %}
    {{ elementary.fabric__edr_hour_of_week_expression(date_expr) }}
{% endmacro %}

{% macro sqlserver__edr_hour_of_day_expression(date_expr) %}
    {{ elementary.fabric__edr_hour_of_day_expression(date_expr) }}
{% endmacro %}

{# ── Full name helpers (use + instead of || for concatenation) ─── #}
{% macro sqlserver__full_table_name(alias) -%}
    {{ elementary.fabric__full_table_name(alias) }}
{%- endmacro %}

{% macro sqlserver__full_schema_name() -%}
    {{ elementary.fabric__full_schema_name() }}
{%- endmacro %}

{% macro sqlserver__full_column_name() -%}
    {{ elementary.fabric__full_column_name() }}
{%- endmacro %}

{% macro sqlserver__full_name_split(part_name) %}
    {{ elementary.fabric__full_name_split(part_name) }}
{% endmacro %}

{# ── Table operations ───────────────────────────────────────────── #}
{% macro sqlserver__insert_as_select(table_relation, select_query) %}
    {{ elementary.fabric__insert_as_select(table_relation, select_query) }}
{% endmacro %}

{% macro sqlserver__edr_make_temp_relation(base_relation, suffix) %}
    {{ elementary.fabric__edr_make_temp_relation(base_relation, suffix) }}
{% endmacro %}

{% macro sqlserver__get_relation_max_name_length(temporary, relation, sql_query) %}
    {{
        elementary.fabric__get_relation_max_name_length(
            temporary, relation, sql_query
        )
    }}
{% endmacro %}

{# ── Boolean / config ───────────────────────────────────────────── #}
{% macro sqlserver__edr_boolean_literal(value) %}
    {{ elementary.fabric__edr_boolean_literal(value) }}
{% endmacro %}

{% macro sqlserver__dummy_values() %}
    {{ return(elementary.fabric__dummy_values()) }}
{% endmacro %}

{# ── String concatenation ──────────────────────────────────────── #}
{% macro sqlserver__list_concat_with_separator(
    item_list, separator, handle_nulls=true
) %}
    {{
        return(
            elementary.fabric__list_concat_with_separator(
                item_list, separator, handle_nulls
            )
        )
    }}
{% endmacro %}

{# ── Surrogate key ─────────────────────────────────────────────── #}
{%- macro sqlserver__generate_surrogate_key(fields) -%}
    {{ elementary.fabric__generate_surrogate_key(fields) }}
{%- endmacro -%}

{# ── Incremental strategy ──────────────────────────────────────── #}
{%- macro sqlserver__get_default_incremental_strategy() %}
    {{ return(elementary.fabric__get_default_incremental_strategy()) }}
{%- endmacro -%}

{# ── Multi-value IN ────────────────────────────────────────────── #}
{%- macro sqlserver__edr_multi_value_in(source_cols, target_cols, target_table) -%}
    {{ elementary.fabric__edr_multi_value_in(source_cols, target_cols, target_table) }}
{%- endmacro -%}

{# ── Data type utilities ───────────────────────────────────────── #}
{% macro sqlserver__data_type_list(data_type) %}
    {{ return(elementary.fabric__data_type_list(data_type)) }}
{% endmacro %}

{% macro sqlserver__get_normalized_data_type(exact_data_type) %}
    {{ return(elementary.fabric__get_normalized_data_type(exact_data_type)) }}
{% endmacro %}

{% macro sqlserver__try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {{
        return(
            elementary.fabric__try_cast_column_to_timestamp(
                table_relation, timestamp_column
            )
        )
    }}
{% endmacro %}

{# ── Insert rows ───────────────────────────────────────────────── #}
{%- macro sqlserver__escape_special_chars(string_value) -%}
    {{ elementary.fabric__escape_special_chars(string_value) }}
{%- endmacro -%}

{%- macro sqlserver__render_value(value, data_type) -%}
    {{ elementary.fabric__render_value(value, data_type) }}
{%- endmacro -%}

{# ── Monitoring queries ────────────────────────────────────────── #}
{% macro sqlserver__column_monitoring_group_by(
    timestamp_column, dimensions, prefixed_dimensions
) %}
    {{
        elementary.fabric__column_monitoring_group_by(
            timestamp_column, dimensions, prefixed_dimensions
        )
    }}
{% endmacro %}

{% macro sqlserver__get_unified_metrics_query(table_metrics, metric_properties) %}
    {{ elementary.fabric__get_unified_metrics_query(table_metrics, metric_properties) }}
{% endmacro %}

{% macro sqlserver__row_count_metric_query(metric, metric_properties) %}
    {{ elementary.fabric__row_count_metric_query(metric, metric_properties) }}
{% endmacro %}

{% macro sqlserver__freshness_metric_query(metric, metric_properties) %}
    {{ elementary.fabric__freshness_metric_query(metric, metric_properties) }}
{% endmacro %}

{% macro sqlserver__event_freshness_metric_query(metric, metric_properties) %}
    {{ elementary.fabric__event_freshness_metric_query(metric, metric_properties) }}
{% endmacro %}

{% macro sqlserver__get_latest_full_refresh(model_node) %}
    {{ elementary.fabric__get_latest_full_refresh(model_node) }}
{% endmacro %}

{# ── Test utilities ────────────────────────────────────────────── #}
{% macro sqlserver__get_failed_row_count_calc_query(failed_row_count_calc) %}
    {{ elementary.fabric__get_failed_row_count_calc_query(failed_row_count_calc) }}
{% endmacro %}

{% macro sqlserver__query_test_result_rows(
    sample_limit=none, ignore_passed_tests=false
) %}
    {{
        return(
            elementary.fabric__query_test_result_rows(
                sample_limit=sample_limit, ignore_passed_tests=ignore_passed_tests
            )
        )
    }}
{% endmacro %}

{%- macro sqlserver__get_anomaly_query(flattened_test=none) -%}
    {{ elementary.fabric__get_anomaly_query(flattened_test=flattened_test) }}
{%- endmacro -%}

{%- macro sqlserver__get_anomaly_query_for_dimension_anomalies(flattened_test=none) -%}
    {{
        elementary.fabric__get_anomaly_query_for_dimension_anomalies(
            flattened_test=flattened_test
        )
    }}
{%- endmacro -%}

{% macro sqlserver__complete_buckets_cte(
    time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
) %}
    {{
        elementary.fabric__complete_buckets_cte(
            time_bucket, bucket_end_expr, min_bucket_start_expr, max_bucket_end_expr
        )
    }}
{% endmacro %}

{# ── Column monitors ───────────────────────────────────────────── #}
{% macro sqlserver__max_length(column_name) -%}
    {{ elementary.fabric__max_length(column_name) }}
{%- endmacro %}
{% macro sqlserver__min_length(column_name) -%}
    {{ elementary.fabric__min_length(column_name) }}
{%- endmacro %}
{% macro sqlserver__average_length(column_name) -%}
    {{ elementary.fabric__average_length(column_name) }}
{%- endmacro %}
{% macro sqlserver__standard_deviation(column_name) -%}
    {{ elementary.fabric__standard_deviation(column_name) }}
{%- endmacro %}
{% macro sqlserver__variance(column_name) -%}
    {{ elementary.fabric__variance(column_name) }}
{%- endmacro %}

{# ── Schema changes ────────────────────────────────────────────── #}
{% macro sqlserver__schema_changes_query_group_by() %}
    {{ elementary.fabric__schema_changes_query_group_by() }}
{% endmacro %}

{% macro sqlserver__schema_change_description_column() %}
    {{ elementary.fabric__schema_change_description_column() }}
{% endmacro %}

{% macro sqlserver__get_column_changes_from_baseline_cur(
    model_relation, full_table_name, model_baseline_relation
) %}
    {{
        elementary.fabric__get_column_changes_from_baseline_cur(
            model_relation, full_table_name, model_baseline_relation
        )
    }}
{% endmacro %}
