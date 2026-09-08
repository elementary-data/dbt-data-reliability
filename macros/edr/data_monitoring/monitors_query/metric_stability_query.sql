{# Compare each settled bucket's current value with its previous/first retained
   measurement. Missing current measurements fail only within the rescan window. #}
{% macro metric_stability_query(
    test_metrics_table_relations,
    full_table_name,
    metric_names,
    metric_properties,
    detection_end,
    days_back,
    min_bucket_age,
    max_change_percent=0,
    change_since=["last_check"],
    column_names=none,
    data_monitoring_metrics_table=none,
    measurement_windows=none
) %}
    {%- if not data_monitoring_metrics_table %}
        {%- set data_monitoring_metrics_table = elementary.get_elementary_relation(
            "data_monitoring_metrics"
        ) %}
    {%- endif %}

    {%- set bucket_period = metric_properties.time_bucket.period %}

    {# Only compare settled buckets within days_back. #}
    {%- set age_kwargs = {min_bucket_age.period ~ "s": min_bucket_age.count} %}
    {%- set max_bucket_end = detection_end - modules.datetime.timedelta(**age_kwargs) %}
    {%- set min_bucket_end = detection_end - modules.datetime.timedelta(
        days=days_back | int
    ) %}
    {%- set max_bucket_end_expr = elementary.edr_date_trunc(
        bucket_period,
        elementary.edr_cast_as_timestamp(
            elementary.edr_datetime_to_sql(max_bucket_end)
        ),
    ) %}
    {%- set min_bucket_end_expr = elementary.edr_date_trunc(
        bucket_period,
        elementary.edr_cast_as_timestamp(
            elementary.edr_datetime_to_sql(min_bucket_end)
        ),
    ) %}
    {# Baselines must also have been measured after the bucket settled. #}
    {%- set settled_at_expr = elementary.edr_cast_as_timestamp(
        elementary.edr_timeadd(
            min_bucket_age.period, min_bucket_age.count, "bucket_end"
        )
    ) %}
    {% set history_window %}
        bucket_end > {{ min_bucket_end_expr }}
        and bucket_end <= {{ max_bucket_end_expr }}
        and updated_at >= {{ settled_at_expr }}
    {% endset %}

    {# Suppress floating-point aggregation noise; zero baselines are handled separately. #}
    {%- set change_percent_noise_floor = 0.000000001 %}
    {%- set change_threshold = "%.10f" | format(
        [max_change_percent, change_percent_noise_floor] | max
    ) %}

    {%- set exceeds_conditions = [] %}
    {%- set baseline_columns = [] %}
    {%- for baseline in change_since %}
        {%- set baseline_column = baseline ~ "_value" %}
        {%- if baseline_column not in baseline_columns %}
            {%- do baseline_columns.append(baseline_column) %}
            {% set exceeds_condition %}
                ({{ baseline_column }} is not null and (
                    ({{ baseline_column }} = 0 and measured_value != 0)
                    or ({{ baseline_column }} != 0 and
                        {{ elementary.metric_stability_change_percent(baseline_column) }} > {{ change_threshold }})
                ))
            {% endset %}
            {% do exceeds_conditions.append(exceeds_condition) %}
        {%- endif %}
    {%- endfor %}

    {%- set metric_stability_query %}
        with metrics_history as (

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value,
                   0 as is_current
            from {{ data_monitoring_metrics_table }}
            where
                upper(full_table_name) = upper('{{ full_table_name }}')
                {%- if column_names %}
                    {#- metric_properties does not carry the column, so without
                        this a test picks up history for every other column
                        monitored on the same table with the same properties. -#}
                    and upper(column_name) in {{ elementary.strings_list_to_tuple(column_names | map("upper") | list) }}
                {%- endif %}
                and metric_name in {{ elementary.strings_list_to_tuple(metric_names) }}
                and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
                and {{ history_window }}
                {# History outside a column's actual rescan cannot establish
                   absence. Restrict it before selecting the newest version. #}
                {%- if measurement_windows %}
                    and (
                    {%- for window in measurement_windows %}
                        (upper(column_name) = upper({{ elementary.edr_quote(window.column_name) }})
                         and bucket_start >= {{ window.min_bucket_start }}
                         and bucket_end <= {{ window.max_bucket_end }})
                        {% if not loop.last %} or {% endif %}
                    {%- endfor %}
                    )
                {%- endif %}

            {%- for test_metrics_table_relation in test_metrics_table_relations %}

            union all

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value,
                   1 as is_current
            from {{ test_metrics_table_relation }}
            where {{ history_window }}
            {%- endfor %}

        ),

        versioned_metrics as (

            {#- Each measurement carries the values it is compared against,
                named after the `change_since` baseline it serves. None of these
                names is reused as an output alias of the final select:
                ClickHouse resolves a select alias anywhere in the same select
                list, so an output `metric_value` shadows the source column for
                its sibling expressions and reports NULL baselines. -#}
            select
                id, full_table_name, column_name, metric_name, metric_type,
                bucket_start, bucket_end, bucket_duration_hours,
                updated_at, dimension, dimension_value, is_current,
                metric_value as measured_value,
                {{ elementary.lag("metric_value") }} over (
                    partition by id order by updated_at
                ) as last_check_value,
                {{ elementary.lag("updated_at") }} over (
                    partition by id order by updated_at
                ) as last_check_at,
                first_value(metric_value) over (
                    partition by id order by updated_at
                    rows between unbounded preceding and current row
                ) as first_check_value,
                first_value(updated_at) over (
                    partition by id order by updated_at
                    rows between unbounded preceding and current row
                ) as first_check_at,
                row_number() over (
                    partition by id order by updated_at desc
                ) as recency
            from metrics_history

        ),

        latest_measurement as (

            {#- One row per bucket: its newest measurement, carrying the values
                it is being compared against. -#}
            select * from versioned_metrics where recency = 1

        )

        {#- This select is frozen into a table, so every timestamp it carries out
            of the metric history is cast to the precision that table accepts.
            Athena reads timestamp(6) from the history and writes millisecond
            columns, and rejects the CTAS otherwise. -#}
        select
            id as metric_id,
            full_table_name,
            column_name,
            metric_name,
            metric_type,
            {{ elementary.edr_cast_as_timestamp("bucket_start") }} as bucket_start,
            {{ elementary.edr_cast_as_timestamp("bucket_end") }} as bucket_end,
            bucket_duration_hours,
            dimension,
            dimension_value,
            {{ elementary.edr_cast_as_timestamp(
                "case when is_current = 0"
                ~ " then " ~ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string()))
                ~ " else updated_at end"
            ) }} as measured_at,
            case when is_current = 0 then 'missing_bucket' else 'value_changed' end as change_type,
            case when is_current = 1 then measured_value end as metric_value,
            case when is_current = 0 then measured_value else last_check_value end as previous_value,
            {{ elementary.edr_cast_as_timestamp(
                "case when is_current = 0 then updated_at else last_check_at end"
            ) }} as previous_measured_at,
            {{ elementary.edr_cast_as_timestamp("first_check_at") }}
            as initial_measured_at,
            first_check_value as initial_value,
            case when is_current = 1 then measured_value - last_check_value end as change_since_last_check,
            case when is_current = 1 then measured_value - first_check_value end as change_since_first_check,
            case
                when is_current = 1 and last_check_value is not null and last_check_value != 0
                then {{ elementary.metric_stability_change_percent("last_check_value") }}
            end as change_percent_since_last_check,
            case
                when is_current = 1 and first_check_value is not null and first_check_value != 0
                then {{ elementary.metric_stability_change_percent("first_check_value") }}
            end as change_percent_since_first_check
        from latest_measurement
        where is_current = 0 or {{ exceeds_conditions | join(" or ") }}
    {%- endset %}
    {%- do return(metric_stability_query) %}
{% endmacro %}


{#
  Relative change from a baseline column, in percentage points. Shared by the
  WHERE predicate and the reported columns so the two cannot drift apart.
#}
{% macro metric_stability_change_percent(baseline_column) -%}
    abs(measured_value - {{ baseline_column }}) / abs({{ baseline_column }}) * 100.0
{%- endmacro %}
