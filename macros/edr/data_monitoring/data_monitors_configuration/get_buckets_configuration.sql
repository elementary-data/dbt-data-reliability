-- TODO: We currently have a consistency problem with buckets like 4 hours, 3 days. We might miss / overlap buckets.
-- Possible solution - Only support month, week, day, and 1 / 2 / 4 / 6 / 12 hourly buckets

{% macro get_min_bucket_start(days_back) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(days_back | int)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

{% macro get_trunc_min_bucket_start_expr(metric_properties, min_bucket_start) %}
    {%- set trunc_min_bucket_start_expr = elementary.edr_date_trunc(metric_properties.time_bucket.period, elementary.edr_cast_as_timestamp(elementary.edr_quote(min_bucket_start)))%}
    {{ return(trunc_min_bucket_start_expr) }}
{% endmacro %}

-- TODO: This needs to be truncated according to the latest full bucket
{% macro get_max_bucket_end() %}
    {% do return(elementary.run_started_at_as_string()) %}
{% endmacro %}

-- TODO: This needs to be truncated according to full buckets?
{% macro get_backfill_bucket_start(backfill_days, metric_properties) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

-- TODO: Not sure I need this
{% macro get_backfill_duration(backfill_days, metric_properties) %}
    {%- set bucket_period_hours = { 'hour': 1,
                                    'day': 24,
                                    'week': 168,
                                    'month': 720
                                   } %}
    {%- set bucket_duration = bucket_period_hours.get(metric_properties.time_bucket.period) * metric_properties.time_bucket.count %}
    {%- set backfill_duration = backfill_days * 24 %}
    {%- set buckets_in_backfill =
        1 if (backfill_duration/bucket_duration <= 1) else (backfill_duration/bucket_duration)
    %}
    {% set rounded_backfill_buckets = buckets_in_backfill | round| int %}
    {%- set backfill_buckets =
        rounded_backfill_buckets if (rounded_backfill_buckets >= buckets_in_backfill) else rounded_backfill_buckets + 1
    %}
    {{ return(backfill_buckets) }}
{% endmacro %}

-- TODO: If backfill_start is before last run, I need to do last_run-backfill_start = hours_gap --> hours_gap/bucket_duration = buckets_to_backfill --> last_run - buckets_to_backfill = backfill_start
{% macro get_test_min_bucket_start(model_graph_node, backfill_days, days_back, monitors=none, column_name=none, metric_properties=none) %}

    {%- set min_bucket_start = elementary.get_min_bucket_start(days_back) %}
    {# We assume we should also cosider sources as incremental #}
    {% if not elementary.is_incremental_model(model_graph_node, source_included=true) %}
        {% do return(min_bucket_start) %}
    {% endif %}

    {%- set trunc_min_bucket_start_expr = elementary.get_trunc_min_bucket_start_expr(metric_properties, min_bucket_start) %}
    {%- set backfill_bucket_start = elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.get_backfill_bucket_start(backfill_days))) %}
    {%- set full_table_name = elementary.model_node_to_full_name(model_graph_node) %}
    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with bucket_times as (
            select min(last_bucket_end) as last_run_of_metric,
                   min(first_bucket_end) as first_run_of_metric,
                   {{ trunc_min_bucket_start_expr }} as days_back_start,
                   {{ backfill_bucket_start }} as backfill_start,
                   {{ elementary.edr_datediff('min(last_bucket_end)', backfill_bucket_start, metric_properties.time_bucket.period) }} as buckets_to_backfill
            from {{ ref('monitors_runs') }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
              and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            )
        select
            case
                {# This prevents gaps in buckets for the metric #}
                when last_run_of_metric is null then days_back_start {# This is the first run of this metric #}
                when last_run_of_metric < days_back_start then days_back_start {# The metric was not collected for a period longer than days_back #}
                when first_run_of_metric > days_back_start then days_back_start {# The metric was collected recently, but for a period that is smaller than days_back #}
                when last_run_of_metric < backfill_start then last_run_of_metric {# The metric was not collected for a period longer than backfill_days #}
                else backfill_start
            end as min_bucket_start
        from bucket_times
    {%- endset %}

    {%- set min_bucket_start_query_result = elementary.result_value(min_bucket_start_query) %}

    {%- if min_bucket_start_query_result %}
        {{ return(min_bucket_start_query_result) }}
    {%- else %}
        {{ return(min_bucket_start) }}
    {%- endif %}

{% endmacro %}