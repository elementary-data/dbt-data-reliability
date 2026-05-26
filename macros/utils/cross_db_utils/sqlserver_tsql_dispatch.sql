{#-
  dbt-sqlserver no longer declares dbt-fabric as a dispatch parent (dbt-msft/dbt-sqlserver#628,
  merged May 2026). T-SQL-safe implementations live in fabric__ macros; sqlserver__ delegates
  to them explicitly so both adapters share the same behavior without relying on adapter deps.

  Do not add sqlserver__ overrides here for macros that already have dedicated sqlserver__
  implementations (e.g. get_normalized_data_type, generate_elementary_profile_args).
-#}
{% macro sqlserver__dummy_values() %}
    {% do return(elementary.fabric__dummy_values()) %}
{% endmacro %}

{% macro sqlserver__edr_type_bool() %}
    {% do return(elementary.fabric__edr_type_bool()) %}
{% endmacro %}

{% macro sqlserver__edr_type_string() %}
    {% do return(elementary.fabric__edr_type_string()) %}
{% endmacro %}

{% macro sqlserver__edr_type_long_string() %}
    {% do return(elementary.fabric__edr_type_long_string()) %}
{% endmacro %}

{% macro sqlserver__edr_type_timestamp() %}
    {{ elementary.fabric__edr_type_timestamp() }}
{% endmacro %}

{% macro sqlserver__edr_boolean_literal(value) %}
    {{ elementary.fabric__edr_boolean_literal(value) }}
{% endmacro %}

{% macro sqlserver__edr_is_true(expr) %}
    {{ elementary.fabric__edr_is_true(expr) }}
{% endmacro %}

{% macro sqlserver__edr_is_false(expr) %}
    {{ elementary.fabric__edr_is_false(expr) }}
{% endmacro %}

{% macro sqlserver__is_tsql() %}
    {% do return(elementary.fabric__is_tsql()) %}
{% endmacro %}

{% macro sqlserver__data_type_list(data_type) %}
    {% do return(elementary.fabric__data_type_list(data_type)) %}
{% endmacro %}

{% macro sqlserver__try_cast_column_to_timestamp(table_relation, timestamp_column) %}
    {{
        elementary.fabric__try_cast_column_to_timestamp(
            table_relation, timestamp_column
        )
    }}
{% endmacro %}

{% macro sqlserver__replace_table_data(relation, rows) %}
    {% do elementary.fabric__replace_table_data(relation, rows) %}
{% endmacro %}

{% macro sqlserver__edr_make_temp_relation(base_relation, suffix) %}
    {% do return(elementary.fabric__edr_make_temp_relation(base_relation, suffix)) %}
{% endmacro %}

{% macro sqlserver__escape_special_chars(string_value) %}
    {{- elementary.fabric__escape_special_chars(string_value) -}}
{% endmacro %}

{% macro sqlserver__insert_as_select(table_relation, select_query) %}
    {% do elementary.fabric__insert_as_select(table_relation, select_query) %}
{% endmacro %}

{% macro sqlserver__get_relation_max_name_length(temporary, relation, sql_query) %}
    {% do return(
        elementary.fabric__get_relation_max_name_length(
            temporary, relation, sql_query
        )
    ) %}
{% endmacro %}

{% macro sqlserver__create_temp_table(
    database_name, schema_name, table_name, sql_query
) %}
    {% do elementary.fabric__create_temp_table(
        database_name, schema_name, table_name, sql_query
    ) %}
{% endmacro %}

{% macro sqlserver__edr_to_char(column, format) %}
    {{ elementary.fabric__edr_to_char(column, format) }}
{% endmacro %}

{% macro sqlserver__edr_timeadd(date_part, number, timestamp_expression) %}
    {{ elementary.fabric__edr_timeadd(date_part, number, timestamp_expression) }}
{% endmacro %}

{% macro sqlserver__edr_time_trunc(date_part, date_expression) %}
    {{ elementary.fabric__edr_time_trunc(date_part, date_expression) }}
{% endmacro %}

{% macro sqlserver__edr_multi_value_in(source_cols, target_cols, target_table) %}
    {{-
        elementary.fabric__edr_multi_value_in(
            source_cols, target_cols, target_table
        )
    -}}
{% endmacro %}

{% macro sqlserver__get_default_incremental_strategy() %}
    {% do return(elementary.fabric__get_default_incremental_strategy()) %}
{% endmacro %}

{% macro sqlserver__edr_hour_of_week_expression(date_expr) %}
    {{ elementary.fabric__edr_hour_of_week_expression(date_expr) }}
{% endmacro %}

{% macro sqlserver__edr_hour_of_day_expression(date_expr) %}
    {{ elementary.fabric__edr_hour_of_day_expression(date_expr) }}
{% endmacro %}

{% macro sqlserver__edr_day_of_week_expression(date_expr) %}
    {{ elementary.fabric__edr_day_of_week_expression(date_expr) }}
{% endmacro %}

{% macro sqlserver__edr_datediff(first_date, second_date, date_part) %}
    {{ elementary.fabric__edr_datediff(first_date, second_date, date_part) }}
{% endmacro %}

{% macro sqlserver__edr_dateadd(datepart, interval, from_date_or_timestamp) %}
    {{ elementary.fabric__edr_dateadd(datepart, interval, from_date_or_timestamp) }}
{% endmacro %}

{% macro sqlserver__edr_date_trunc(datepart, date_expression) %}
    {{ elementary.fabric__edr_date_trunc(datepart, date_expression) }}
{% endmacro %}

{% macro sqlserver__edr_current_timestamp() -%}
    {{ elementary.fabric__edr_current_timestamp() }}
{%- endmacro %}

{% macro sqlserver__edr_current_timestamp_in_utc() -%}
    {{ elementary.fabric__edr_current_timestamp_in_utc() }}
{%- endmacro %}

{% macro sqlserver__edr_concat(fields) %}
    {{ elementary.fabric__edr_concat(fields) }}
{% endmacro %}

{% macro sqlserver__full_name_split(part_name) %}
    {% do return(elementary.fabric__full_name_split(part_name)) %}
{% endmacro %}

{% macro sqlserver__complete_buckets_cte(
    time_bucket,
    bucket_end_expr,
    min_bucket_start_expr,
    max_bucket_end_expr
) -%}
    {{-
        elementary.fabric__complete_buckets_cte(
            time_bucket,
            bucket_end_expr,
            min_bucket_start_expr,
            max_bucket_end_expr,
        )
    -}}
{%- endmacro %}

{% macro sqlserver__get_failed_row_count_calc_query(failed_row_count_calc) %}
    {% do return(
        elementary.fabric__get_failed_row_count_calc_query(
            failed_row_count_calc
        )
    ) %}
{% endmacro %}

{% macro sqlserver__get_column_changes_from_baseline_cur(
    model_relation, full_table_name, model_baseline_relation
) %}
    {% do elementary.fabric__get_column_changes_from_baseline_cur(
        model_relation, full_table_name, model_baseline_relation
    ) %}
{% endmacro %}

{% macro sqlserver___bucket_end_freshness_expr() %}
    {{ elementary.fabric___bucket_end_freshness_expr() }}
{% endmacro %}

{% macro sqlserver__freshness_metric_query(metric, metric_properties) %}
    {{ elementary.fabric__freshness_metric_query(metric, metric_properties) }}
{% endmacro %}

{% macro sqlserver__get_latest_full_refresh(model_node) %}
    {{ elementary.fabric__get_latest_full_refresh(model_node) }}
{% endmacro %}

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

{%- macro sqlserver__get_anomaly_query(flattened_test=none) -%}
    {{- elementary.fabric__get_anomaly_query(flattened_test) -}}
{%- endmacro -%}

{%- macro sqlserver__get_anomaly_query_for_dimension_anomalies(flattened_test=none) -%}
    {{- elementary.fabric__get_anomaly_query_for_dimension_anomalies(flattened_test) -}}
{%- endmacro -%}

{% macro sqlserver__query_test_result_rows(
    sample_limit=none, ignore_passed_tests=false
) %}
    {% do return(
        elementary.fabric__query_test_result_rows(
            sample_limit, ignore_passed_tests
        )
    ) %}
{% endmacro %}
