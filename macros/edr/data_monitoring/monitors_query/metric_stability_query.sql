{#
  Detects metrics whose value for an already-observed time bucket has changed
  since a previous run.

  Standard anomaly detection compares different buckets at one point in time.
  This compares one bucket against its own earlier measurements, which is a
  different axis and a far lower noise floor: for settled data the expected
  change is zero.

  The version history this reads is already collected. `data_monitoring_metrics`
  is append-only (rows are inserted by the on-run-end hook), and a metric `id`
  hashes the table, column, metric name and bucket_end while deliberately
  excluding `updated_at` and `metric_value`. So re-measuring a bucket appends a
  new row, and the earlier measurements remain.
#}
{% macro metric_stability_query(
    test_metrics_table_relation,
    full_table_name,
    metric_names,
    metric_properties,
    detection_end,
    min_bucket_age=none,
    max_change_percent=0,
    change_since=["last_check"],
    column_name=none,
    data_monitoring_metrics_table=none
) %}
    {%- if not data_monitoring_metrics_table %}
        {%- set data_monitoring_metrics_table = elementary.get_elementary_relation(
            "data_monitoring_metrics"
        ) %}
    {%- endif %}

    {#- Only evaluate buckets old enough to be considered settled. Recent data is
        expected to keep moving (late arrivals, unsettled records), so comparing it
        produces noise rather than signal. -#}
    {%- if min_bucket_age %}
        {%- set age_kwargs = {min_bucket_age.period ~ "s": min_bucket_age.count} %}
        {%- set max_bucket_end = detection_end - modules.datetime.timedelta(
            **age_kwargs
        ) %}
    {%- else %} {%- set max_bucket_end = detection_end %}
    {%- endif %}
    {%- set max_bucket_end_expr = elementary.edr_cast_as_timestamp(
        elementary.edr_datetime_to_sql(max_bucket_end)
    ) %}

    {#- A move away from exactly zero is always a change: the relative form is
        undefined there, so it is handled explicitly rather than dividing by zero. -#}
    {%- set exceeds_conditions = [] %}
    {%- if "last_check" in change_since %}
        {%- do exceeds_conditions.append(
            "(previous_value is not null and case"
            ~ " when previous_value = 0 then metric_value != 0"
            ~ " else abs(metric_value - previous_value) / abs(previous_value) * 100.0 > "
            ~ max_change_percent
            ~ " end)"
        ) %}
    {%- endif %}
    {%- if "first_check" in change_since %}
        {%- do exceeds_conditions.append(
            "(initial_value is not null and case"
            ~ " when initial_value = 0 then metric_value != 0"
            ~ " else abs(metric_value - initial_value) / abs(initial_value) * 100.0 > "
            ~ max_change_percent
            ~ " end)"
        ) %}
    {%- endif %}
    {%- if not exceeds_conditions %}
        {%- do exceptions.raise_compiler_error(
            "`change_since` must contain at least one of 'last_check', 'first_check'."
        ) %}
    {%- endif %}

    {%- set metric_stability_query %}
        with metrics_history as (

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value
            from {{ data_monitoring_metrics_table }}
            where
                upper(full_table_name) = upper('{{ full_table_name }}')
                and metric_name in {{ elementary.strings_list_to_tuple(metric_names) }}
                and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
                and bucket_end <= {{ max_bucket_end_expr }}
                {%- if column_name %}
                    and upper(column_name) = upper('{{ column_name }}')
                {%- endif %}

            union all

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value
            from {{ test_metrics_table_relation }}
            where bucket_end <= {{ max_bucket_end_expr }}

        ),

        versioned_metrics as (

            select
                id, full_table_name, column_name, metric_name, metric_type,
                bucket_start, bucket_end, bucket_duration_hours,
                metric_value, updated_at, dimension, dimension_value,
                {{ elementary.lag("metric_value") }} over (
                    partition by id order by updated_at
                ) as previous_value,
                {{ elementary.first_value("metric_value") }} over (
                    partition by id order by updated_at
                    rows between unbounded preceding and current row
                ) as initial_value,
                row_number() over (
                    partition by id order by updated_at desc
                ) as recency
            from metrics_history

        ),

        latest_measurement as (

            {#- One row per bucket: its newest measurement, carrying the values it
                is being compared against. -#}
            select * from versioned_metrics where recency = 1

        )

        select
            id as metric_id,
            full_table_name,
            column_name,
            metric_name,
            metric_type,
            bucket_start,
            bucket_end,
            bucket_duration_hours,
            dimension,
            dimension_value,
            updated_at as measured_at,
            metric_value,
            previous_value,
            initial_value,
            metric_value - previous_value as change_since_last_check,
            metric_value - initial_value as change_since_first_check
        from latest_measurement
        where {{ exceeds_conditions | join(" or ") }}
    {%- endset %}
    {%- do return(metric_stability_query) %}
{% endmacro %}
