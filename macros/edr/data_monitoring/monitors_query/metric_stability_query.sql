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
    max_change_absolute=none,
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

    {# A relative threshold suits magnitudes, whose scale varies by orders of
       magnitude between models. It does not suit ratios: a margin already sits
       on a fixed scale, so the move that transfers is the absolute one. And
       because the relative form divides by the baseline, its sensitivity is
       recomputed per bucket, so a setting meaning 20bp at a 45% margin means
       about 9bp at 20% and 27bp at 60%. max_change_absolute expresses the move
       directly.

       Both legs are written as `moved > threshold * scale`, with no division,
       so neither needs a zero-baseline escape: at a zero baseline the relative
       leg reduces to `moved > 0`, which is satisfied by any change rather than
       falsified by it, and the absolute leg decides on its own. The earlier
       form built a separate predicate per branch, which is how the zero
       baseline was suppressed once already. #}
    {# Two coefficients, both scaled by the operands, because the errors they
       cover scale with the operands too.

       noise_floor stands in for an absolute threshold when none was given, so
       that re-aggregating the same rows in a different order does not read as
       a restatement. It is the old 1e-9 percent floor written as a fraction,
       so the default behaviour is unchanged. It is deliberately NOT added on
       top of an explicit max_change_absolute: setting one states the
       resolution wanted, and folding the floor into it dropped real moves on
       large metrics (a move of 2 on a daily sum of 2.5e11, with
       max_change_absolute: 1, went unreported).

       comparison_tolerance absorbs the representation error in the comparison
       itself. It is about 4 ulps of a double, which is the smallest value that
       still covers the boundary cases below with room to spare (measured: 30x
       the worst observed excess, over magnitudes from 0.1 to 1e12). Sizing it
       more loosely masks real moves on large metrics: at 1e-13 a baseline of
       1e12 carries a tolerance of 0.2, which hides a move of 0.01 under an
       absolute threshold of 0.001. A decimal threshold has no exact binary form, so 45.30 -> 45.10
       is 0.19999999999999574 while 45.50 -> 45.30 is 0.20000000000000284, and
       without it the same 20bp move is reported in one direction only. A fixed
       slack cannot do this, because that error grows with the operands: at a
       bucket of 12345678.30 it reaches 1e-9, five orders of magnitude above a
       slack sized for values around 45. #}
    {%- set noise_floor = 0.00000000001 %}
    {%- set comparison_tolerance = 0.000000000000001 %}

    {#- Thresholds render through `string`, python's shortest round-tripping
        form, so the literal in the SQL is the double that was configured.
        Fixed decimals are not: at "%.10f" a tolerance survived on a 0-100
        scale and vanished on a 0-1 scale, so the same move behaved differently
        depending on how the metric happened to be scaled. -#}
    {%- set percent_fraction = (max_change_percent / 100.0) | string %}
    {%- set tolerance_coefficient = comparison_tolerance | string %}
    {%- set absolute_literal = (
        max_change_absolute | string
        if max_change_absolute is not none
        else none
    ) %}

    {%- set exceeds_conditions = [] %}
    {%- set baseline_columns = [] %}
    {%- for baseline in change_since %}
        {%- set baseline_column = baseline ~ "_value" %}
        {%- if baseline_column not in baseline_columns %}
            {%- do baseline_columns.append(baseline_column) %}
            {%- set moved = "abs(measured_value - " ~ baseline_column ~ ")" %}
            {%- set scale = "(abs(measured_value) + abs(" ~ baseline_column ~ "))" %}
            {%- set tolerance = tolerance_coefficient ~ " * " ~ scale %}
            {%- set absolute_leg = (
                absolute_literal
                if absolute_literal is not none
                else (noise_floor | string) ~ " * " ~ scale
            ) %}
            {% set exceeds_condition %}
                ({{ baseline_column }} is not null
                 and {{ moved }} > {{ percent_fraction }} * abs({{ baseline_column }}) + {{ tolerance }}
                 and {{ moved }} > {{ absolute_leg }} + {{ tolerance }})
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
            {#- nullif inside the helper already yields NULL at a zero or
                missing baseline, so is_current is the only guard left. -#}
            case
                when is_current = 1
                then {{ elementary.metric_stability_change_percent("last_check_value") }}
            end as change_percent_since_last_check,
            case
                when is_current = 1
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

  nullif keeps this NULL rather than raising at a zero baseline. SQL does not
  guarantee that a `= 0` guard around the call is evaluated first, so on an
  engine that evaluates both sides of an OR the division would raise.
#}
{% macro metric_stability_change_percent(baseline_column) -%}
    abs(measured_value - {{ baseline_column }})
    / nullif(abs({{ baseline_column }}), 0)
    * 100.0
{%- endmacro %}
