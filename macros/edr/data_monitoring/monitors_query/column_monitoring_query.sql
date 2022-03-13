{% macro column_monitoring_query(monitored_table) %}

    {%- set timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set column_monitors_list = elementary.all_column_monitors() %}

    {%- if execute %}
    {%- set table_config = get_table_monitoring_config(monitored_table) %}
    {%- set full_table_name = elementary.insensitive_get_dict_value(table_config, 'full_table_name') %}
    {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
    {%- set is_timestamp = elementary.insensitive_get_dict_value(table_config, 'is_timestamp') %}
    {%- set column_config = elementary.insensitive_get_dict_value(table_config, 'columns_config') %}
    {%- set timeframe_start = "'"~ elementary.insensitive_get_dict_value(table_config, 'timeframe_start') ~"'" %}
    {%- endif %}


    with timeframe_data as (

        select *
            {% if is_timestamp -%}
             , {{ elementary.date_trunc('day', timestamp_column) }} as edr_bucket
            {%- else %}
       , null as edr_bucket
            {%- endif %}
        from {{ elementary.from(full_table_name) }}
        where
        {% if is_timestamp -%}
            {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(timeframe_start) }}
            and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(timeframe_end) }}
        {%- else %}
            true
        {%- endif %}

    ),

    column_monitors as (

        {%- if column_config %}
            {%- for monitored_column in column_config -%}
                {%- set column_name = monitored_column.get('column_name') -%}
                {%- set column_monitors = monitored_column.get('column_monitors') -%}
                {%- set column = elementary.column_quote(column_name) -%}
                select
                    edr_bucket,
                    '{{ column_name }}' as edr_column_name,
                    {%- if 'null_count' in column_monitors -%} {{ elementary.null_count(column) }} {%- else -%} null {% endif %} as null_count,
                    {%- if 'null_percent' in column_monitors -%} {{ elementary.null_percent(column) }} {%- else -%} null {% endif %} as null_percent,
                    {%- if 'max' in column_monitors -%} {{ elementary.max(column) }} {%- else -%} null {% endif %} as max,
                    {%- if 'min' in column_monitors -%} {{ elementary.min(column) }} {%- else -%} null {% endif %} as min,
                    {%- if 'average' in column_monitors -%} {{ elementary.average(column) }} {%- else -%} null {% endif %} as average,
                    {%- if 'zero_count' in column_monitors -%} {{ elementary.zero_count(column) }} {%- else -%} null {% endif %} as zero_count,
                    {%- if 'zero_percent' in column_monitors -%} {{ elementary.zero_percent(column) }} {%- else -%} null {% endif %} as zero_percent,
                    {%- if 'standard_deviation' in column_monitors -%} {{ elementary.standard_deviation(column) }} {%- else -%} null {% endif %} as standard_deviation,
                    {%- if 'variance' in column_monitors -%} {{ elementary.variance(column) }} {%- else -%} null {% endif %} as variance,
                    {%- if 'max_length' in column_monitors -%} {{ elementary.max_length(column) }} {%- else -%} null {% endif %} as max_length,
                    {%- if 'min_length' in column_monitors -%} {{ elementary.min_length(column) }} {%- else -%} null {% endif %} as min_length,
                    {%- if 'average_length' in column_monitors -%} {{ elementary.average_length(column) }} {%- else -%} null {% endif %} as average_length,
                    {%- if 'missing_count' in column_monitors -%} {{ elementary.missing_count(column) }} {%- else -%} null {% endif %} as missing_count,
                    {%- if 'missing_percent' in column_monitors -%} {{ elementary.missing_percent(column) }} {%- else -%} null {% endif %} as missing_percent
                from timeframe_data
                group by 1,2
                {% if not loop.last %} union all {% endif %}
        {% endfor %}
    {%- else %}
        {{ elementary.empty_column_monitors_cte() }}
    {%- endif %}

    ),

    column_monitors_unpivot as (

        {% for monitor in column_monitors_list %}
            select edr_column_name, edr_bucket, '{{ monitor }}' as metric_name, {{ elementary.cast_as_float(monitor) }} as metric_value from column_monitors where {{ monitor }} is not null
            {% if not loop.last %} union all {% endif %}
        {%- endfor %}

    ),

    metrics_final as (

        select
            '{{ full_table_name }}' as full_table_name,
            edr_column_name as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }},
            {%- if timestamp_column %}
            edr_bucket as bucket_start,
            {{ elementary.cast_as_timestamp(dbt_utils.dateadd('day',1,'edr_bucket')) }} as bucket_end,
            24 as bucket_duration_hours
            {%- else %}
            {{ elementary.null_timestamp() }} as bucket_start,
            {{ elementary.null_timestamp() }} as bucket_end,
            {{ elementary.null_int() }} as bucket_duration_hours
            {%- endif %}
        from column_monitors_unpivot
        where cast(metric_value as {{ dbt_utils.type_int() }}) < {{ var('max_int') }}

    )

    select * from metrics_final

{% endmacro %}