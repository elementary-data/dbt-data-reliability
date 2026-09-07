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

    {#- A bucket's first measurements are taken while it is still settling, and
        min_bucket_age exists precisely to keep that period out of scope. Left
        in, they become the 'first_check' baseline, so every comparison carries
        the settling as a permanent offset and the drift 'first_check' exists to
        find is buried under it. Measurements are therefore bounded by the same
        age as the buckets. The current run's own measurement always qualifies:
        a bucket is only eligible once bucket_end + min_bucket_age has passed. -#}
    {%- set settled_measurement_window = (
        "updated_at >= "
        ~ elementary.edr_cast_as_timestamp(
            elementary.edr_timeadd(
                min_bucket_age.period, min_bucket_age.count, "bucket_end"
            )
        )
    ) %}
    {%- set history_window = bucket_window ~ " and " ~ settled_measurement_window %}

    {#- Conditions keep booleans in boolean position rather than returning one
        from a CASE, which T-SQL has no first-class value for. A move away from
        exactly zero is handled separately, since the relative form is undefined
        there. -#}
    {#- Repeating a float aggregate can differ in the last bits when the scan is
        partitioned differently between runs, since floating point addition is
        not associative. That is a relative change around 1e-14, which a strict
        comparison against the default of 0 reports as a failure on data nobody
        touched. The floor sits far above that and far below any real movement,
        and leaves the zero-crossing rule below untouched. -#}
    {%- set change_percent_noise_floor = 0.000000001 %}
    {%- set change_threshold = "%.10f" | format(
        [max_change_percent, change_percent_noise_floor] | max
    ) %}

    {%- set exceeds_conditions = [] %}
    {%- set baseline_columns = [] %}
    {%- for baseline in change_since %}
        {%- set baseline_column = (
            "previous_value" if baseline == "last_check" else "initial_value"
        ) %}
        {%- if baseline_column not in baseline_columns %}
            {%- do baseline_columns.append(baseline_column) %}
            {%- do exceeds_conditions.append(
                "("
                ~ baseline_column
                ~ " is not null and (("
                ~ baseline_column
                ~ " = 0 and metric_value != 0) or ("
                ~ baseline_column
                ~ " != 0 and "
                ~ elementary.metric_stability_change_percent(baseline_column)
                ~ " > "
                ~ change_threshold
                ~ ")))"
            ) %}
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

            select
                id, full_table_name, column_name, metric_name, metric_type,
                bucket_start, bucket_end, bucket_duration_hours,
                metric_value, updated_at, dimension, dimension_value, is_current,
                {{ elementary.lag("metric_value") }} over (
                    partition by id order by updated_at
                ) as previous_value,
                {{ elementary.lag("updated_at") }} over (
                    partition by id order by updated_at
                ) as previous_measured_at,
                first_value(updated_at) over (
                    partition by id order by updated_at
                    rows between unbounded preceding and current row
                ) as initial_measured_at,
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
            case when is_current = 0
                 then {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }}
                 else updated_at end as measured_at,
            case when is_current = 0 then 'missing_bucket' else 'value_changed' end as change_type,
            case when is_current = 1 then metric_value end as metric_value,
            case when is_current = 0 then metric_value else previous_value end as previous_value,
            case when is_current = 0 then updated_at else previous_measured_at end as previous_measured_at,
            initial_measured_at,
            initial_value,
            case when is_current = 1 then metric_value - previous_value end as change_since_last_check,
            case when is_current = 1 then metric_value - initial_value end as change_since_first_check,
            case
                when is_current = 1 and previous_value is not null and previous_value != 0
                then {{ elementary.metric_stability_change_percent("previous_value") }}
            end as change_percent_since_last_check,
            case
                when is_current = 1 and initial_value is not null and initial_value != 0
                then {{ elementary.metric_stability_change_percent("initial_value") }}
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
{% macro metric_stability_change_percent(baseline_column) %}
    {%- do return(
        "abs(metric_value - "
        ~ baseline_column
        ~ ") / abs("
        ~ baseline_column
        ~ ") * 100.0"
    ) %}
{% endmacro %}
