{#
  elementary.metric_stability

  Fails when a metric's value for an already-settled time bucket has changed
  since a previous run.

  Regular anomaly detection compares one bucket against neighbouring buckets, so
  it cannot see this: a restatement that shifts many historical buckets together
  moves the training baseline along with the data, and normal period-to-period
  variation is usually far wider than the change being looked for.

  This is a threshold test rather than an anomaly test by design. For settled
  data the expected change is zero, so the metric series has no variance to
  learn from. A relative threshold also transfers across metrics, where an
  absolute one has to be retuned for every metric.

  Arguments:
    columns            - columns to monitor.
    metrics            - metric types to monitor per column (e.g. [sum]).
    timestamp_column   - column that buckets the data into periods.
    change_since       - baselines to compare against: 'last_check' (the previous
                         measurement), 'first_check' (the earliest measurement),
                         or both. 'last_check' catches a sudden correction;
                         'first_check' catches slow drift where no single step is
                         large enough to trip the threshold.
    min_bucket_age     - only check buckets at least this old, e.g.
                         {count: 4, period: week}. Recent data is expected to
                         keep changing, so comparing it produces noise.
    max_change_percent - permitted relative change before failing. Defaults to 0,
                         meaning any change to settled data fails.
#}
{% test metric_stability(
    model,
    columns,
    metrics,
    timestamp_column,
    change_since=["last_check"],
    min_bucket_age=none,
    max_change_percent=0,
    time_bucket=none,
    where_expression=none,
    days_back=none,
    backfill_days=none,
    detection_delay=none,
    dimensions=none
) %}
    {{ config(tags=["elementary-tests"]) }}

    {%- if execute and elementary.is_test_command() and elementary.is_elementary_enabled() %}

        {%- if max_change_percent < 0 %}
            {{
                exceptions.raise_compiler_error(
                    "max_change_percent must be non-negative."
                )
            }}
        {%- endif %}

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

        {#- The comparison needs each bucket measured more than once, so buckets
            must keep being re-measured for as long as they are being checked.
            backfill_days controls that window, and its default of 2 would leave
            nothing to compare for any older bucket. Derive it from min_bucket_age
            so the test cannot silently find nothing. -#}
        {%- set required_backfill_days = (
            elementary.get_metric_stability_backfill_days(
                min_bucket_age, backfill_days
            )
        ) %}

        {% set column_metrics = [] %}
        {% set metric_names = [] %}
        {%- for metric_type in metrics %}
            {% do column_metrics.append({"name": metric_type, "type": metric_type}) %}
            {% do metric_names.append(metric_type) %}
        {%- endfor %}

        {#- Collect this run's metrics. Shared infrastructure handles bucket
            selection, computation, temp table creation and cache storage, and the
            on-run-end hook persists them, which is what builds the history this
            test reads on later runs. -#}
        {%- for column_name in columns %}
            {% do elementary.collect_column_metrics(
                column_metrics=column_metrics,
                model_expr=model,
                model_relation=model_relation,
                column_name=column_name,
                timestamp_column=timestamp_column,
                time_bucket=time_bucket,
                days_back=days_back,
                backfill_days=required_backfill_days,
                where_expression=where_expression,
                dimensions=dimensions,
                collected_by="metric_stability",
            ) %}
        {%- endfor %}

        {% set model_graph_node = elementary.get_model_graph_node(model_relation) %}
        {% set metric_properties = elementary.get_metric_properties(
            model_graph_node,
            timestamp_column,
            where_expression,
            time_bucket,
            dimensions,
            collected_by="metric_stability",
        ) %}

        {% set test_metrics_table = elementary.get_elementary_test_table(
            elementary.get_elementary_test_table_name(), "metrics"
        ) %}
        {% set full_table_name = elementary.relation_to_full_name(model_relation) %}
        {% set detection_end = elementary.get_detection_end(detection_delay) %}

        {% set metric_stability_query = elementary.metric_stability_query(
            test_metrics_table_relation=test_metrics_table,
            full_table_name=full_table_name,
            metric_names=metric_names,
            metric_properties=metric_properties,
            detection_end=detection_end,
            min_bucket_age=min_bucket_age,
            max_change_percent=max_change_percent,
            change_since=change_since,
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


{#
  backfill_days sets how far back buckets are re-measured on each run, and a
  bucket can only be checked while it is still being re-measured. Eligibility
  starts at min_bucket_age, so the window has to reach meaningfully past that
  age or a bucket freezes before it can ever be compared.

  The default keeps watching a bucket for as long again as it took to settle,
  which gives real coverage rather than a single-day overlap, and matters more
  for 'first_check': catching slow drift needs a bucket observed over a stretch,
  not once. Cost scales with this window, since that many days of the model are
  re-scanned each run.

  An explicit backfill_days that cannot produce a comparison is a configuration
  error rather than a silent pass.
#}
{% macro get_metric_stability_backfill_days(min_bucket_age, backfill_days) %}
    {%- if not min_bucket_age %} {%- do return(backfill_days) %} {%- endif %}

    {%- set age_kwargs = {min_bucket_age.period ~ "s": min_bucket_age.count} %}
    {%- set min_age_days = (
        (modules.datetime.timedelta(**age_kwargs).total_seconds() / 86400)
        | round(0, "ceil")
        | int
    ) %}
    {%- set derived = min_age_days * 2 %}
    {#- Absolute floor: at least one day of overlap past the age cutoff. -#}
    {%- set minimum_viable = min_age_days + 1 %}

    {%- if backfill_days is none %} {%- do return(derived) %} {%- endif %}

    {%- if backfill_days < minimum_viable %}
        {%- do exceptions.raise_compiler_error(
            "backfill_days is "
            ~ backfill_days
            ~ ", which is too small to detect changes in buckets at least "
            ~ min_age_days
            ~ " days old: those buckets stop being re-measured before they become eligible to check, so the test would never find a change. Use at least "
            ~ minimum_viable
            ~ " (ideally "
            ~ derived
            ~ "), or remove backfill_days to have it derived automatically."
        ) %}
    {%- endif %}
    {%- do return(backfill_days) %}
{% endmacro %}
