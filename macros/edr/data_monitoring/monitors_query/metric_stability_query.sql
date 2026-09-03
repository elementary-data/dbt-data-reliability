{#
  Detects metrics whose value for an already-settled time bucket has changed
  since a previous run.

  Standard anomaly detection compares different buckets at one point in time.
  This compares one bucket against its own earlier measurements, which is a
  different axis and a far lower noise floor: for settled data the expected
  change is zero.

  This is deliberately a threshold test rather than an anomaly test. A settled
  series has no variance to learn from, and the scoring degenerates in both
  directions. With the value excluded from its own training set the stddev is
  zero and the score is forced to zero, so it never fires. With the value
  included, n unchanged observations followed by one value v give mean v/(n+1)
  and stddev v/sqrt(n+1), so the score is n/sqrt(n+1): the v cancels and the
  score reflects how long the history is rather than how large the change was.

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
    days_back,
    min_bucket_age,
    max_change_percent=0,
    change_since=["last_check"],
    column_names=none,
    data_monitoring_metrics_table=none
) %}
    {%- if not data_monitoring_metrics_table %}
        {%- set data_monitoring_metrics_table = elementary.get_elementary_relation(
            "data_monitoring_metrics"
        ) %}
    {%- endif %}

    {%- set bucket_period = metric_properties.time_bucket.period %}

    {#- Eligible buckets form a band. The upper edge keeps recent data out:
        it is expected to keep moving as late records arrive, so comparing it
        produces noise. The lower edge bounds the read, which both prunes the
        scan (and enables partition pruning) and lets a reported change age out
        of the window instead of failing the test forever. -#}
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
    {%- set bucket_window = (
        "bucket_end > "
        ~ min_bucket_end_expr
        ~ " and bucket_end <= "
        ~ max_bucket_end_expr
    ) %}

    {#- Conditions keep booleans in boolean position rather than returning one
        from a CASE, which T-SQL has no first-class value for. A move away from
        exactly zero is handled separately, since the relative form is undefined
        there. -#}
    {%- set exceeds_conditions = [] %}
    {%- for baseline in change_since %}
        {%- set baseline_column = (
            "previous_value" if baseline == "last_check" else "initial_value"
        ) %}
        {%- do exceeds_conditions.append(
            "("
            ~ baseline_column
            ~ " is not null and (("
            ~ baseline_column
            ~ " = 0 and metric_value != 0) or ("
            ~ baseline_column
            ~ " != 0 and abs(metric_value - "
            ~ baseline_column
            ~ ") / abs("
            ~ baseline_column
            ~ ") * 100.0 > "
            ~ max_change_percent
            ~ ")))"
        ) %}
    {%- endfor %}

    {%- set metric_stability_query %}
        with metrics_history as (

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value
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
                and {{ bucket_window }}

            union all

            select id, full_table_name, column_name, metric_name, metric_type,
                   bucket_start, bucket_end, bucket_duration_hours,
                   metric_value, updated_at, dimension, dimension_value
            from {{ test_metrics_table_relation }}
            where {{ bucket_window }}

        ),

        versioned_metrics as (

            select
                id, full_table_name, column_name, metric_name, metric_type,
                bucket_start, bucket_end, bucket_duration_hours,
                metric_value, updated_at, dimension, dimension_value,
                {{ elementary.lag("metric_value") }} over (
                    partition by id order by updated_at
                ) as previous_value,
                first_value(metric_value) over (
                    partition by id order by updated_at
                    rows between unbounded preceding and current row
                ) as initial_value,
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
            metric_value - initial_value as change_since_first_check,
            case
                when previous_value is not null and previous_value != 0
                then abs(metric_value - previous_value) / abs(previous_value) * 100.0
            end as change_percent_since_last_check,
            case
                when initial_value is not null and initial_value != 0
                then abs(metric_value - initial_value) / abs(initial_value) * 100.0
            end as change_percent_since_first_check
        from latest_measurement
        where {{ exceeds_conditions | join(" or ") }}
    {%- endset %}
    {%- do return(metric_stability_query) %}
{% endmacro %}
