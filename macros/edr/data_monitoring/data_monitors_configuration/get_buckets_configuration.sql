{% macro get_min_bucket_start(days_back)) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(days_back | int)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

-- TODO: This needs to be truncated according to the latest full bucket
{% macro get_max_bucket_end() %}
    {% do return(elementary.run_started_at_as_string()) %}
{% endmacro %}

{% macro get_backfill_bucket_start(backfill_days) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

{% macro get_test_min_bucket_start(model_graph_node, backfill_days, days_back, monitors=none, column_name=none, metric_properties=none) %}
    {%- set min_bucket_start = elementary.get_min_bucket_start(days_back) %}
    {# We assume we should also cosider sources as incremental #}
    {% if not elementary.is_incremental_model(model_graph_node, source_included=true) %}
        {% do return(min_bucket_start) %}
    {% endif %}

    {%- set backfill_bucket_start = elementary.get_backfill_bucket_start(backfill_days) %}
    {% set full_table_name = elementary.model_node_to_full_name(model_graph_node) %}
    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with min_times as (
            select min(last_bucket_end) as last_run_of_metric,
                   min(first_bucket_end) as first_run_of_metric,
                {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(min_bucket_start)) }} as min_start,
                {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(backfill_bucket_start)) }} as backfill_start
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
                when last_run_of_metric is null then min_start {# This is the first run of this metric #}
                when last_run_of_metric < min_start then min_start {# The metric was not collected for a period longer than days_back #}
                when first_run_of_metric > min_start then min_start {# The metric was collected recently, but for a period that is smaller than days_back #}
                when last_run < backfill_start then last_run {# The metric was not collected for a period longer than backfill_days #}
                else backfill_start
            end as min_start
        from min_times
    {%- endset %}

    {%- set min_bucket_start_query_result = elementary.result_value(min_bucket_start_query) %}

    {%- if min_bucket_start_query_result %}
        {{ return(min_bucket_start_query_result) }}
    {%- else %}
        {{ return(min_bucket_start) }}
    {%- endif %}

{% endmacro %}