{% macro get_min_bucket_start() %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back'))).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

{% macro get_min_bucket_end() %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(elementary.get_config_var('days_back') - 1)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}

{% macro get_max_bucket_end() %}
    {% do return(elementary.run_started_at_as_string()) %}
{% endmacro %}

{% macro get_backfill_bucket_start(backfill_days) %}
    {% do return((elementary.get_run_started_at() - modules.datetime.timedelta(backfill_days)).strftime("%Y-%m-%d 00:00:00")) %}
{% endmacro %}


{% macro get_test_min_bucket_start(model_graph_node, backfill_days, monitors=none, column_name=none) %}
    {%- set min_bucket_start = elementary.get_min_bucket_start() %}
    {% if not elementary.is_incremental_model(model_graph_node) %}
        {% do return(min_bucket_start) %}
    {% endif %}

    {%- set backfill_bucket_start = elementary.get_backfill_bucket_start(backfill_days) %}
    {% set full_table_name = elementary.model_node_to_full_name(model_graph_node) %}
    {%- if monitors %}
        {%- set monitors_tuple = elementary.strings_list_to_tuple(monitors) %}
    {%- endif %}

    {%- set min_bucket_start_query %}
        with min_times as (
            select min(last_bucket_end) as last_run,
                {{ elementary.cast_as_timestamp(elementary.quote(min_bucket_start)) }} as min_start,
                {{ elementary.cast_as_timestamp(elementary.quote(backfill_bucket_start)) }} as backfill_start
            from {{ ref('monitors_runs') }}
            where upper(full_table_name) = upper('{{ full_table_name }}')
            {%- if monitors %}
                and metric_name in {{ monitors_tuple }}
            {%- endif %}
            {%- if column_name %}
                and upper(column_name) = upper('{{ column_name }}')
            {%- endif %}
            )
        select
            case
                when last_run is null then min_start
                when last_run < backfill_start then last_run
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