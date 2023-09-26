{% macro get_detection_end(detection_delay) %}
    {%- set kwargs = {detection_delay.period+'s': detection_delay.count} %}
    {%- set detection_end = elementary.get_run_started_at() - modules.datetime.timedelta(**kwargs) %}
    {{ return(detection_end) }}
{% endmacro %}

{% macro get_trunc_min_bucket_start_expr(detection_end, metric_properties, days_back) %}
    {%- set untruncated_min = (detection_end - modules.datetime.timedelta(days_back | int)).strftime("%Y-%m-%d 00:00:00") %}
    {%- set trunc_min_bucket_start_expr = elementary.edr_date_trunc(metric_properties.time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(untruncated_min)))%}
    {{ return(trunc_min_bucket_start_expr) }}
{% endmacro %}

{# This macro can't be used without truncating to full buckets #}
{% macro get_backfill_bucket_start(detection_end, backfill_days) %}
    {% do return((detection_end - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}


{% macro get_test_buckets_min_and_max(model_relation, backfill_days, days_back, detection_delay, monitors=none, column_name=none, metric_properties=none, unit_test=false, unit_test_relation=none) %}

    {%- set detection_end = elementary.get_detection_end(detection_delay) %}
    {%- set detection_end_expr = elementary.edr_cast_as_timestamp(elementary.edr_quote(detection_end)) %}
    {%- set trunc_min_bucket_start_expr = elementary.get_trunc_min_bucket_start_expr(detection_end, metric_properties, days_back) %}
    {%- set backfill_bucket_start = elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_backfill_bucket_start(detection_end, backfill_days))) %}
    {%- set full_table_name = elementary.relation_to_full_name(model_relation) %}
    {%- set force_metrics_backfill = elementary.get_config_var('force_metrics_backfill') %}

    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- if unit_test %}
        {%- set monitors_runs_relation = dbt.load_relation(unit_test_relation) %}
    {%- else %}
        {%- set monitors_runs_relation = ref('monitors_runs') %}
    {%- endif %}

    {%- set regular_bucket_times_query %}
        with bucket_times as (
            select
            {{ trunc_min_bucket_start_expr }} as days_back_start
           , {{ detection_end_expr }} as detection_end
        ),
        full_buckets_calc as (
            select *,
                floor({{ elementary.edr_datediff('days_back_start', 'detection_end', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }} as periods_until_max
            from bucket_times
        )
        select
             days_back_start as min_bucket_start,
             {{ elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'days_back_start') }} {# Add full buckets to last_max_bucket_end #}
        as max_bucket_end
        from full_buckets_calc
    {%- endset %}

    {%- set incremental_bucket_times_query %}
        with bucket_times as (
            select max(last_bucket_end) as max_existing_bucket_end,
                   {{ elementary.edr_cast_as_timestamp(elementary.edr_timeadd(metric_properties.time_bucket.period, -1 * metric_properties.time_bucket.count, 'min(first_bucket_end)')) }} as min_existing_bucket_start,
                   {{ trunc_min_bucket_start_expr }} as days_back_start,
                   {{ backfill_bucket_start }} as backfill_start,
                   {{ detection_end_expr }} as detection_end
            from {{ monitors_runs_relation }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
              and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            ),
        full_buckets_calc as (
            select *,
                {# How many periods we need to reduce from max_existing_bucket_end to backfill full time buckets #}
                case
                    when max_existing_bucket_end is not null
                    then least(ceil({{ elementary.edr_datediff('max_existing_bucket_end', 'backfill_start', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}), -1) * {{ metric_properties.time_bucket.count }}
                    else 0
                end as periods_to_backfill,
                {# How many periods we need to add to last run time to get only full time buckets #}
                case
                    when max_existing_bucket_end is not null and max_existing_bucket_end > days_back_start and min_existing_bucket_start <= days_back_start
                    then floor({{ elementary.edr_datediff('max_existing_bucket_end', 'detection_end', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }}
                    else floor({{ elementary.edr_datediff('days_back_start', 'detection_end', metric_properties.time_bucket.period) }} / {{ metric_properties.time_bucket.count }}) * {{ metric_properties.time_bucket.count }}
                end as periods_until_max
            from bucket_times
        )
        select
            case
                {# This prevents gaps in buckets for the metric #}
                when max_existing_bucket_end is null then days_back_start {# When this is the first run of this metric #}
                when max_existing_bucket_end <= days_back_start then days_back_start {# When the metric was not collected for a period longer than days_back #}
                when min_existing_bucket_start > days_back_start then days_back_start {# When the metric was collected recently, but for a period that is smaller than days_back #}
                when max_existing_bucket_end <= backfill_start then max_existing_bucket_end {# When the metric was not collected for a period longer than backfill_days #}
                else {{ elementary.edr_cast_as_timestamp(elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_to_backfill', 'max_existing_bucket_end')) }} {# When backfill reduce full time buckets from max_existing_bucket_end to backfill #}
            end as min_bucket_start,
            case
                {# This makes sure we collect only full bucket #}
                when max_existing_bucket_end is null or max_existing_bucket_end <= days_back_start or min_existing_bucket_start > days_back_start
                then {{ elementary.edr_cast_as_timestamp(elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'days_back_start')) }} {# Add full buckets to days_back_start #}
                else {{ elementary.edr_cast_as_timestamp(elementary.edr_timeadd(metric_properties.time_bucket.period, 'periods_until_max', 'max_existing_bucket_end')) }} {# Add full buckets to max_existing_bucket_end #}
            end as max_bucket_end
        from full_buckets_calc
    {%- endset %}

    {# We assume we should also cosider sources as incremental #}
    {% if force_metrics_backfill or not (elementary.is_incremental_model(elementary.get_model_graph_node(model_relation), source_included=true) or unit_test) %}
        {%- set buckets = elementary.agate_to_dicts(elementary.run_query(regular_bucket_times_query))[0] %}
    {%- else %}
        {%- set buckets = elementary.agate_to_dicts(elementary.run_query(incremental_bucket_times_query))[0] %}
    {% endif %}

    {%- if buckets %}
        {%- set min_bucket_start = elementary.edr_quote(buckets.get('min_bucket_start')) %}
        {%- set max_bucket_end = elementary.edr_quote(buckets.get('max_bucket_end')) %}
        {{ return([min_bucket_start, max_bucket_end]) }}
    {%- else %}
        {{ exceptions.raise_compiler_error("Failed to calc test buckets min and max") }}
    {%- endif %}

{% endmacro %}
