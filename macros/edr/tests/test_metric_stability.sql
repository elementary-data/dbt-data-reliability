{# Detect restatements of settled bucket metrics within the observation window.
   Configuration, baseline behavior, and limits:
   https://docs.elementary-data.com/data-tests/metric-stability #}
{% test metric_stability(
    model,
    columns,
    metrics,
    timestamp_column,
    min_bucket_age,
    change_since=["last_check"],
    max_change_percent=0,
    time_bucket=none,
    where_expression=none,
    days_back=none,
    backfill_days=none,
    dimensions=none
) %}
    {{ config(tags=["elementary-tests"]) }}

    {% if not (
        execute
        and elementary.is_test_command()
        and elementary.is_elementary_enabled()
    ) %}
        {% do return(elementary.no_results_query()) %}
    {% endif %}

    {% set arguments = elementary._parse_and_validate_metric_stability_arguments(
        model,
        columns,
        metrics,
        timestamp_column,
        min_bucket_age,
        change_since,
        max_change_percent,
        days_back,
        backfill_days,
    ) %}
    {% set model_relation = arguments.model_relation %}
    {% set model_graph_node = arguments.model_graph_node %}
    {% set timestamp_column = arguments.timestamp_column %}
    {% set metrics = arguments.metrics %}
    {% set change_since = arguments.change_since %}
    {%- if not dimensions %} {% set dimensions = [] %} {%- endif %}

    {% set metric_properties = elementary.get_metric_properties(
        model_graph_node,
        timestamp_column,
        where_expression,
        time_bucket,
        dimensions,
        collected_by="metric_stability",
    ) %}
    {% set metric_names = metrics %}

    {# Size the scan from the settling age and bucket duration. #}
    {% set resolved_window = elementary.resolve_metric_stability_window(
        model_graph_node,
        min_bucket_age,
        metric_properties.time_bucket,
        days_back,
        backfill_days,
    ) %}
    {% set days_back = resolved_window["days_back"] %}
    {% set backfill_days = resolved_window["backfill_days"] %}

    {% set test_table_name = elementary.get_elementary_test_table_name() %}
    {% set (
        database_name,
        schema_name,
    ) = elementary.get_package_database_and_schema("elementary") %}
    {% set tests_schema_name = elementary.get_elementary_tests_schema(
        database_name, schema_name
    ) %}
    {% set full_table_name = elementary.relation_to_full_name(model_relation) %}

    {% set collected = elementary._collect_metric_stability_metrics(
        model,
        model_relation,
        arguments.columns,
        metric_properties,
        days_back,
        backfill_days,
        dimensions,
        database_name,
        tests_schema_name,
        test_table_name,
    ) %}
    {% set detection_end = elementary.get_detection_end(none) %}
    {% set metric_stability_query = elementary.metric_stability_query(
        test_metrics_table_relations=collected.relations,
        full_table_name=full_table_name,
        metric_names=metric_names,
        metric_properties=metric_properties,
        detection_end=detection_end,
        days_back=days_back,
        min_bucket_age=min_bucket_age,
        max_change_percent=max_change_percent,
        change_since=change_since,
        column_names=collected.columns,
        measurement_windows=collected.windows,
    ) %}
    {{ elementary.debug_log("metric_stability_query - \n" ~ metric_stability_query) }}

    {# Freeze failures before dbt executes the test and samples its results.
       Use the normal sampling path so sample limits and privacy settings
       still apply. This relation is cleaned up with the metrics tables. #}
    {% set result_relation = elementary.create_elementary_test_table(
        database_name,
        tests_schema_name,
        test_table_name,
        "stability_results",
        metric_stability_query,
    ) %}
    select *
    from {{ result_relation }}

{% endtest %}

{# Collect one persistent CTAS table per column and return relations, scan
   windows, and resolved names. Tables are unioned when comparing history. #}
{% macro _collect_metric_stability_metrics(
    model,
    model_relation,
    column_definitions,
    metric_properties,
    days_back,
    backfill_days,
    dimensions,
    database_name,
    tests_schema_name,
    test_table_name
) %}
    {% set temp_table_relations = [] %}
    {% set measurement_windows = [] %}
    {% set resolved_columns = [] %}

    {%- for column_obj_and_monitors in column_definitions %}
        {% set resolved_column = column_obj_and_monitors["column"].name %}
        {% do resolved_columns.append(resolved_column) %}
        {% set column_monitors = column_obj_and_monitors["monitors"] %}

        {%- set (
            raw_min_bucket_start,
            max_bucket_end,
        ) = elementary.get_metric_buckets_min_and_max(
            model_relation=model_relation,
            backfill_days=backfill_days,
            days_back=days_back,
            metric_names=column_monitors,
            column_name=resolved_column,
            metric_properties=metric_properties,
        ) %}
        {# Keep bucket identities stable across runs, including weekly buckets. #}
        {%- set min_bucket_start = elementary.edr_date_trunc(
            metric_properties.time_bucket.period,
            elementary.edr_cast_as_timestamp(raw_min_bucket_start),
        ) %}
        {%- do measurement_windows.append(
            {
                "column_name": resolved_column,
                "min_bucket_start": min_bucket_start,
                "max_bucket_end": elementary.edr_cast_as_timestamp(
                    max_bucket_end
                ),
            }
        ) %}
        {%- set this_column_metrics = [] %}
        {%- for monitor in column_monitors %}
            {%- do this_column_metrics.append({"name": monitor, "type": monitor}) %}
        {%- endfor %}
        {%- set column_monitoring_query = elementary.column_monitoring_query(
            model,
            model_relation,
            min_bucket_start,
            max_bucket_end,
            days_back,
            column_obj_and_monitors["column"],
            this_column_metrics,
            metric_properties,
            dimensions,
        ) %}
        {%- do temp_table_relations.append(
            elementary.create_elementary_test_table(
                database_name,
                tests_schema_name,
                test_table_name,
                "metrics_" ~ loop.index0,
                column_monitoring_query,
            )
        ) %}
    {%- endfor %}

    {# Register every column table for history persistence at on-run-end. #}
    {% set metrics_tables_cache = (
        elementary.get_cache("tables").get("metrics").get("relations")
    ) %}
    {%- for temp_table_relation in temp_table_relations %}
        {% do metrics_tables_cache.append(temp_table_relation) %}
    {%- endfor %}

    {% do return(
        {
            "relations": temp_table_relations,
            "windows": measurement_windows,
            "columns": resolved_columns,
        }
    ) %}
{% endmacro %}

{% macro validate_min_bucket_age(min_bucket_age) %}
    {%- set valid_periods = ["day", "week", "hour", "minute", "second"] %}
    {%- if not min_bucket_age or min_bucket_age is not mapping %}
        {% do exceptions.raise_compiler_error(
            "min_bucket_age is required and must be a mapping. Expected format:   min_bucket_age:     count: int     period: string"
        ) %}
    {%- endif %}
    {%- for key in min_bucket_age %}
        {%- if key not in ["count", "period"] %}
            {% do exceptions.raise_compiler_error(
                "Found invalid key in min_bucket_age: '"
                ~ key
                ~ "'. Supported keys: count, period."
            ) %}
        {%- endif %}
    {%- endfor %}
    {%- if min_bucket_age.period not in valid_periods %}
        {% do exceptions.raise_compiler_error(
            "Unsupported min_bucket_age period '"
            ~ min_bucket_age.period
            ~ "'. Supported periods: "
            ~ valid_periods
            | join(", ")
            ~ ". time_bucket also accepts month, quarter and year; express an age over those in days, e.g. {count: 60, period: day}."
        ) %}
    {%- endif %}
    {%- if min_bucket_age.count is not integer or min_bucket_age.count < 1 %}
        {% do exceptions.raise_compiler_error(
            "min_bucket_age count must be a positive integer, got '"
            ~ min_bucket_age.count
            ~ "'."
        ) %}
    {%- endif %}
{% endmacro %}


{# Return days_back/backfill_days sized for settled buckets; reject windows
   below the required minimum. Backfill applies to sources/incremental models. #}
{% macro resolve_metric_stability_window(
    model_graph_node, min_bucket_age, time_bucket, days_back, backfill_days
) %}
    {%- set age_kwargs = {min_bucket_age.period ~ "s": min_bucket_age.count} %}
    {%- set age_days = (
        modules.datetime.timedelta(**age_kwargs).total_seconds() / 86400.0
    ) %}

    {%- if time_bucket.count | int != 1 %}
        {% do exceptions.raise_compiler_error(
            "metric_stability requires a time_bucket count of 1, got "
            ~ time_bucket.count
            ~ ". A multi-step bucket cannot be measured on a stable grid across runs, so the test would never report a change."
        ) %}
    {%- endif %}

    {# Calendar periods use nominal day lengths for sizing only. #}
    {%- set period_days = {
        "second": 1.0 / 86400.0,
        "minute": 1.0 / 1440.0,
        "hour": 1.0 / 24.0,
        "day": 1.0,
        "week": 7.0,
        "month": 30.0,
        "quarter": 91.0,
        "year": 365.0,
    } %}
    {%- set bucket_days = period_days.get(time_bucket.period | lower) %}
    {%- if not bucket_days %}
        {% do exceptions.raise_compiler_error(
            "Unsupported time_bucket period for metric_stability: '"
            ~ time_bucket.period
            ~ "'."
        ) %}
    {%- endif %}

    {# The window has to outlast settling, or nothing is ever both settled and
       still measured. Take the widest of three floors:
         - twice the age, so a bucket is observed over a stretch of runs rather
           than measured once and never compared;
         - two whole buckets past the age, or the settled band is narrower than
           a single bucket;
         - one whole day past the age. days_back is counted in whole days and
           the metrics scan starts at midnight, so this keeps a sub-day age from
           collapsing the window to a sliver of a day, and it is what makes the
           floor at least 2 for every age, so a days_back the query would
           truncate to 0 or 1 days is rejected below instead of silently
           producing an empty comparison window. #}
    {%- set twice_the_age = (age_days * 2) | round(0, "ceil") | int %}
    {%- set two_buckets_past_age = (
        (age_days + 2 * bucket_days) | round(0, "ceil") | int
    ) %}
    {%- set one_day_past_age = (age_days + 1) | round(0, "ceil") | int %}
    {%- set derived = [twice_the_age, two_buckets_past_age, one_day_past_age] | max %}
    {%- set age_description = (
        min_bucket_age.count
        ~ " "
        ~ min_bucket_age.period
        ~ ("s" if min_bucket_age.count > 1 else "")
    ) %}

    {%- set uses_backfill_window = elementary.is_incremental_model(
        model_graph_node, source_included=true
    ) and not elementary.get_config_var("force_metrics_backfill") %}

    {%- if days_back is none %} {%- set resolved_days_back = derived %}
    {%- else %}
        {%- set resolved_days_back = days_back %}
        {%- if resolved_days_back < derived %}
            {% do exceptions.raise_compiler_error(
                "days_back is "
                ~ resolved_days_back
                ~ ", which does not leave room for whole buckets past a min_bucket_age of "
                ~ age_description
                ~ ", so no bucket is ever both settled and still measured and the test can never report a change. Use at least "
                ~ derived
                ~ ", or omit days_back to have it derived."
            ) %}
        {%- endif %}
    {%- endif %}

    {%- if backfill_days is none %}
        {%- set resolved_backfill_days = resolved_days_back %}
    {%- else %}
        {%- set resolved_backfill_days = backfill_days %}
        {%- if uses_backfill_window and resolved_backfill_days < derived %}
            {% do exceptions.raise_compiler_error(
                "backfill_days is "
                ~ resolved_backfill_days
                ~ ", which does not extend past a min_bucket_age of "
                ~ age_description
                ~ ". On incremental models and sources backfill_days sets how far back buckets are re-measured, so those buckets freeze before they become eligible to check. Use at least "
                ~ derived
                ~ ", or omit backfill_days to have it derived."
            ) %}
        {%- endif %}
    {%- endif %}

    {%- do return(
        {
            "days_back": resolved_days_back,
            "backfill_days": resolved_backfill_days,
        }
    ) %}
{% endmacro %}


{# Validate and normalize arguments; return resolved model, timestamp, and columns. #}
{% macro _parse_and_validate_metric_stability_arguments(
    model,
    columns,
    metrics,
    timestamp_column,
    min_bucket_age,
    change_since,
    max_change_percent,
    days_back,
    backfill_days
) %}
    {%- if columns is string %} {% set columns = [columns] %} {%- endif %}
    {%- if metrics is string %} {% set metrics = [metrics] %} {%- endif %}
    {%- if change_since is string %}
        {% set change_since = [change_since] %}
    {%- endif %}
    {%- if columns %}
        {%- set seen_columns = [] %}
        {%- set deduped_columns = [] %}
        {%- for column_name in columns %}
            {%- set key = column_name | trim('"') | lower %}
            {%- if key not in seen_columns %}
                {%- do seen_columns.append(key) %}
                {%- do deduped_columns.append(column_name) %}
            {%- endif %}
        {%- endfor %}
        {%- set columns = deduped_columns %}
    {%- endif %}

    {%- if not change_since %}
        {{
            exceptions.raise_compiler_error(
                "metric_stability requires at least one baseline in `change_since`: 'last_check', 'first_check', or both."
            )
        }}
    {%- endif %}

    {%- if max_change_percent is not number %}
        {{
            exceptions.raise_compiler_error(
                "max_change_percent must be a number, got '"
                ~ max_change_percent
                ~ "'. Write it unquoted, e.g. max_change_percent: 25."
            )
        }}
    {%- endif %}
    {%- if max_change_percent < 0 %}
        {{
            exceptions.raise_compiler_error(
                "max_change_percent must be non-negative."
            )
        }}
    {%- endif %}
    {%- for arg_name, arg_value in [
        ("days_back", days_back),
        ("backfill_days", backfill_days),
    ] %}
        {%- if arg_value is not none and arg_value is not number %}
            {{
                exceptions.raise_compiler_error(
                    arg_name
                    ~ " must be a number, got '"
                    ~ arg_value
                    ~ "'. Write it unquoted, e.g. "
                    ~ arg_name
                    ~ ": 30."
                )
            }}
        {%- endif %}
    {%- endfor %}

    {%- if not columns %}
        {{
            exceptions.raise_compiler_error(
                "metric_stability requires at least one column in `columns`."
            )
        }}
    {%- endif %}

    {%- if not metrics %}
        {{
            exceptions.raise_compiler_error(
                "metric_stability requires at least one metric type in `metrics`."
            )
        }}
    {%- endif %}

    {%- set available_column_monitors = elementary.get_available_column_monitors() %}
    {%- for metric_type in metrics %}
        {%- if metric_type not in available_column_monitors %}
            {{
                exceptions.raise_compiler_error(
                    "Unsupported column metric: '"
                    ~ metric_type
                    ~ "'. Supported metrics are: "
                    ~ available_column_monitors
                    | join(", ") ~ "."
                )
            }}
        {%- endif %}
    {%- endfor %}

    {%- for baseline in change_since %}
        {%- if baseline not in ["last_check", "first_check"] %}
            {{
                exceptions.raise_compiler_error(
                    "Unsupported `change_since` value '"
                    ~ baseline
                    ~ "'. Supported values are 'last_check' and 'first_check'."
                )
            }}
        {%- endif %}
    {%- endfor %}

    {% do elementary.validate_min_bucket_age(min_bucket_age) %}

    {% set model_relation = elementary.get_model_relation_for_test(
        model, elementary.get_test_model()
    ) %}
    {%- if not model_relation %}
        {{ exceptions.raise_compiler_error("Unsupported model: " ~ model) }}
    {%- endif %}

    {%- if elementary.is_ephemeral_model(model_relation) %}
        {{
            exceptions.raise_compiler_error(
                "Test not supported for ephemeral models: "
                ~ model_relation.identifier
            )
        }}
    {%- endif %}

    {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
    {% set timestamp_column = elementary.get_test_argument(
        "timestamp_column", timestamp_column, model_graph_node
    ) %}
    {%- if not timestamp_column %}
        {{
            exceptions.raise_compiler_error(
                "metric_stability requires a `timestamp_column`, either on the test or in the model's elementary config."
            )
        }}
    {%- endif %}

    {% set timestamp_column_data_type = (
        elementary.find_normalized_data_type_for_column(
            model_relation, timestamp_column
        )
    ) %}
    {%- if not elementary.is_column_timestamp(
        model_relation, timestamp_column, timestamp_column_data_type
    ) %}
        {{
            exceptions.raise_compiler_error(
                "Column '"
                ~ timestamp_column
                ~ "' is not a timestamp type. metric_stability buckets data over time and requires a timestamp column."
            )
        }}
    {%- endif %}

    {% set full_table_name = elementary.relation_to_full_name(model_relation) %}
    {% set column_definitions = [] %}
    {% for column_name in columns %}
        {%- set column_obj_and_monitors = elementary.get_column_obj_and_monitors(
            model_relation, column_name, metrics
        ) -%}
        {%- if not column_obj_and_monitors %}
            {{
                exceptions.raise_compiler_error(
                    "Unable to find column `"
                    ~ column_name
                    ~ "` in `"
                    ~ full_table_name
                    ~ "`."
                )
            }}
        {%- endif %}
        {%- set resolved_column = column_obj_and_monitors["column"].name %}
        {%- set column_monitors = column_obj_and_monitors["monitors"] %}
        {%- if not column_monitors %}
            {{
                exceptions.raise_compiler_error(
                    "None of the metrics " ~ metrics
                    | join(", ")
                    ~ " apply to column `"
                    ~ column_name
                    ~ "` given its data type."
                )
            }}
        {%- endif %}

        {% do column_definitions.append(column_obj_and_monitors) %}
    {% endfor %}
    {% do return(
        {
            "columns": column_definitions,
            "metrics": metrics,
            "change_since": change_since,
            "model_relation": model_relation,
            "model_graph_node": model_graph_node,
            "timestamp_column": timestamp_column,
        }
    ) %}
{% endmacro %}
