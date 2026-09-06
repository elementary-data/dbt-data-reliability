{#
  elementary.metric_stability

  Fails when a metric's value for an already-settled time bucket has changed
  since a previous run.

  Regular anomaly detection compares one bucket against neighbouring buckets, so
  it cannot see this: a restatement that shifts many historical buckets together
  moves the training baseline along with the data, and normal period-to-period
  variation is usually far wider than the change being looked for.

  Arguments:
    columns            - columns to monitor.
    metrics            - metric types to monitor per column (e.g. [sum]).
    timestamp_column   - column that buckets the data into periods.
    min_bucket_age     - required. Only check buckets at least this old, e.g.
                         {count: 4, period: week}. Recent data is expected to
                         keep changing as late records arrive, so comparing it
                         reports noise rather than restatements.
    change_since       - baselines to compare against: 'last_check' (the previous
                         measurement), 'first_check' (the earliest measurement),
                         or both. 'last_check' catches a sudden correction;
                         'first_check' catches slow drift, where each step is too
                         small to trip the threshold but the total movement is
                         not.
    max_change_percent - permitted change in percentage points before failing
                         (25 means 25%). Defaults to 0, so any change to settled
                         data fails.
    days_back          - how far back buckets are measured and compared. This is
                         the observation window, and it must extend past
                         min_bucket_age or no bucket is ever both settled and
                         still being measured.

  Choosing min_bucket_age:

    A bucket is compared against its own earlier measurements, so it needs
    several of them before 'first_check' says anything 'last_check' does not.
    The count is roughly (days_back - min_bucket_age) / run interval, and
    days_back is derived from min_bucket_age, so an age close to the run
    interval leaves only two measurements and the two baselines collapse into
    the same comparison. Set min_bucket_age to a multiple of how often the
    project runs, not to the smallest age that looks settled.

  Limitations:

    A bucket that loses all of its rows produces no new measurement at all,
    rather than a measurement of zero, so the newest value stays whatever it
    was and the change is not reported. Partial deletion is caught normally,
    since the metric moves. Pair this with a volume test if whole periods can
    disappear.
#}
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

    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}

        {#- yaml lets a single value be written as a scalar, and iterating a
            string in jinja walks it character by character. -#}
        {%- if columns is string %} {% set columns = [columns] %} {%- endif %}
        {%- if metrics is string %} {% set metrics = [metrics] %} {%- endif %}
        {%- if change_since is string %}
            {% set change_since = [change_since] %}
        {%- endif %}
        {#- Column lookup is case-insensitive, so a duplicate spelling would
            otherwise be collected twice under one metric id and make the
            'last_check' baseline this run's own second measurement. -#}
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

        {#- Comparing a string against 0 raises a bare Python TypeError, which
            surfaces as an unreadable stack trace rather than a config error. -#}
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

        {%- set available_column_monitors = (
            elementary.get_available_column_monitors()
        ) %}
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
        {#- timestamp_column is commonly set once in the model's elementary
            config rather than repeated on every test. -#}
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

        {#- The measurement window has to extend past min_bucket_age and has to
            be wide enough to hold whole buckets, so it is derived from both
            rather than falling back to defaults that would leave nothing to
            compare. -#}
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

        {#- One shared metrics table for every column. collect_column_metrics
            would create a table per column and leave the cache pointing at the
            last one, so all but the final column would be compared against
            stale measurements.

            Each column gets its own table, created directly from its select.
            Creating one table empty and filling it with INSERT statements
            loses the rows on adapters where dbt rolls back the test's
            transaction, which leaves data_monitoring_metrics with no history
            at all and makes this test a silent permanent pass. The tables are
            unioned at read time instead. -#}
        {% set temp_table_relations = [] %}

        {%- for column_name in columns %}
            {%- set column_obj_and_monitors = (
                elementary.get_column_obj_and_monitors(
                    model_relation, column_name, metrics
                )
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

            {%- set (
                raw_min_bucket_start,
                max_bucket_end,
            ) = elementary.get_metric_buckets_min_and_max(
                model_relation=model_relation,
                backfill_days=backfill_days,
                days_back=days_back,
                metric_names=column_monitors,
                column_name=column_name,
                metric_properties=metric_properties,
            ) %}
            {#- get_metric_buckets_min_and_max can return a plain midnight
                (backfill_bucket_start), which the bucket grid is then anchored
                on. For any period longer than a day that midnight moves with
                the run, so the grid drifts, every bucket_end lands on a new
                surrogate id and no bucket is ever measured twice. Snapping the
                anchor to the bucket period keeps ids stable across runs. -#}
            {%- set min_bucket_start = elementary.edr_date_trunc(
                metric_properties.time_bucket.period,
                elementary.edr_cast_as_timestamp(raw_min_bucket_start),
            ) %}
            {#- Only the monitors that apply to this column's data type.
                Passing the full list would generate e.g. sum(<string column>). -#}
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

        {#- Persist this run's measurements, which is what builds the history
            the next run compares against. store_metrics_table_in_cache only
            knows about a single "metrics" table, so the per-column relations
            are registered directly. -#}
        {% set metrics_tables_cache = (
            elementary.get_cache("tables").get("metrics").get("relations")
        ) %}
        {%- for temp_table_relation in temp_table_relations %}
            {% do metrics_tables_cache.append(temp_table_relation) %}
        {%- endfor %}

        {% set detection_end = elementary.get_detection_end(none) %}
        {% set metric_stability_query = elementary.metric_stability_query(
            test_metrics_table_relations=temp_table_relations,
            full_table_name=full_table_name,
            metric_names=metric_names,
            metric_properties=metric_properties,
            detection_end=detection_end,
            days_back=days_back,
            min_bucket_age=min_bucket_age,
            max_change_percent=max_change_percent,
            change_since=change_since,
            column_names=columns,
        ) %}
        {{
            elementary.debug_log(
                "metric_stability_query - \n" ~ metric_stability_query
            )
        }}

        {{ metric_stability_query }}

    {%- else %}

        {#- test must run an sql query -#}
        {{ elementary.no_results_query() }}

    {%- endif %}
{% endtest %}


{% macro validate_min_bucket_age(min_bucket_age) %}
    {%- set valid_periods = ["day", "week", "hour", "minute", "second"] %}
    {%- if not min_bucket_age or min_bucket_age is not mapping %}
        {# fmt: off #}
        {% do exceptions.raise_compiler_error(
            "min_bucket_age is required and must be a mapping. Expected format:   min_bucket_age:     count: int     period: string"
        ) %}
    {# fmt: on #}
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


{#
  A bucket can only be compared while it is still being re-measured, so the
  measurement window has to extend past min_bucket_age or nothing is ever both
  settled and still under observation.

  Which parameter governs that window depends on the materialization.
  get_metric_buckets_min_and_max takes its incremental branch for sources and
  incremental models, where backfill_days sets the window; every other model
  takes the regular branch, which re-measures the whole days_back window and
  ignores backfill_days entirely. days_back additionally bounds the comparison
  itself, so it always matters.

  Unset parameters are derived from min_bucket_age, because the package defaults
  (days_back 14, backfill_days 2) are unrelated to how long a bucket needs
  watching and would silently leave nothing to compare. An explicit value too
  small to ever produce a comparison raises instead.
#}
{% macro resolve_metric_stability_window(
    model_graph_node, min_bucket_age, time_bucket, days_back, backfill_days
) %}
    {%- set age_kwargs = {min_bucket_age.period ~ "s": min_bucket_age.count} %}
    {#- Kept as a fraction of a day. Ceiling it first would turn a sub-day age
        into a whole day and reject a days_back that in fact covers many
        settled buckets. -#}
    {%- set age_days = (
        modules.datetime.timedelta(**age_kwargs).total_seconds() / 86400.0
    ) %}

    {#- The grid anchor moves by a day between runs, so a bucket spanning more
        than one period step cannot be given a stable identity and the test
        would silently never fire. Refuse rather than pass forever. -#}
    {%- if time_bucket.count | int != 1 %}
        {% do exceptions.raise_compiler_error(
            "metric_stability requires a time_bucket count of 1, got "
            ~ time_bucket.count
            ~ ". A multi-step bucket cannot be measured on a stable grid across runs, so the test would never report a change."
        ) %}
    {%- endif %}

    {#- Bucket length in days, used to make sure the eligible band can actually
        hold whole buckets. month/quarter/year are nominal: they only have to be
        good enough to size the window. -#}
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

    {#- Twice the age, so a bucket is observed over a stretch rather than for a
        single run, which is what lets 'first_check' see drift accumulate; and
        at least two whole buckets past the age, or the settled band is narrower
        than one bucket and nothing is ever both settled and still measured. -#}
    {%- set derived = [
        (age_days * 2) | round(0, "ceil") | int,
        (age_days + 2 * bucket_days) | round(0, "ceil") | int,
        (age_days + 1) | round(0, "ceil") | int,
        1,
    ] | max %}
    {%- set age_description = (
        min_bucket_age.count
        ~ " "
        ~ min_bucket_age.period
        ~ ("s" if min_bucket_age.count > 1 else "")
    ) %}

    {#- get_metric_buckets_min_and_max only takes its backfill branch when
        force_metrics_backfill is off; with it on every model re-measures the
        whole days_back window and backfill_days is ignored, so validating it
        would abort the run over a value that has no effect. -#}
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
