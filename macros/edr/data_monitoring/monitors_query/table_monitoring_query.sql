{% macro table_monitoring_query(monitored_table) %}

    {%- set max_bucket_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set table_monitors_list = ['row_count'] %}

    {%- if execute %}
    {%- set table_config = get_table_monitoring_config(monitored_table) %}
    {%- set full_table_name = elementary.insensitive_get_dict_value(table_config, 'full_table_name') %}
    {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
    {%- set is_timestamp = elementary.insensitive_get_dict_value(table_config, 'is_timestamp') %}
    {%- set table_monitors = elementary.insensitive_get_dict_value(table_config, 'final_table_monitors') | list %}
    {%- set timeframe_start = "'"~ elementary.insensitive_get_dict_value(table_config, 'timeframe_start') ~"'" %}
    {%- endif %}

    with timeframe_data as (

        select *
        {% if is_timestamp -%}
             , {{ elementary.date_trunc('day', timestamp_column) }} as edr_bucket
        {%- else %}
            , {{ elementary.null_timestamp() }} as edr_bucket
        {%- endif %}
        from {{ elementary.from(full_table_name) }}
        where
        {% if is_timestamp -%}
            {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(timeframe_start) }}
            and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
        {%- else %}
            true
        {%- endif %}

    ),

    daily_buckets as (

        {{ elementary.daily_buckets_cte() }}
        where edr_daily_bucket >= {{ elementary.cast_as_timestamp(timeframe_start) }} and edr_daily_bucket <= {{ elementary.cast_as_timestamp(max_bucket_end) }}

    ),

    table_monitors as (

    {%- if 'row_count' in table_monitors %}
        select edr_daily_bucket, edr_bucket,
            {%- if 'row_count' in table_monitors %} case when edr_bucket is null then 0 else {{ elementary.row_count() }} end {%- else -%} {{ elementary.null_float() }} {% endif %} as row_count
        from daily_buckets left join timeframe_data on (edr_daily_bucket = edr_bucket)
        group by 1,2
            {%- else %}
                {{ elementary.empty_table([('edr_daily_bucket','timestamp'),('edr_bucket','timestamp'),('row_count','int')]) }}
            {%- endif %}

    ),

    table_monitors_unpivot as (

        {% for monitor in table_monitors_list %}
            select edr_daily_bucket as edr_bucket, '{{ monitor }}' as metric_name, {{ elementary.cast_as_float(monitor) }} as metric_value from table_monitors where {{ monitor }} is not null
            {% if not loop.last %} union all {% endif %}
        {%- endfor %}

    ),

    table_freshness as (

    {%- if 'freshness' in table_monitors and is_timestamp %}
        select
            edr_daily_bucket as edr_bucket,
            'freshness' as metric_name,
            {{ elementary.timediff('minute', 'max('~timestamp_column~')', dbt_utils.dateadd('day','1','edr_daily_bucket')) }} as metric_value
        from daily_buckets, {{ elementary.from(full_table_name) }}
        where {{ timestamp_column }} <= {{ dbt_utils.dateadd('day','1','edr_daily_bucket') }}
        group by 1,2
            {%- else %}
            {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','str'),('metric_value','int')]) }}
            {%- endif %}

    ),

    union_metrics as (

        select * from table_monitors_unpivot
        union all
        select * from table_freshness

    ),

    metrics_final as (

        select
            '{{ full_table_name }}' as full_table_name,
            {{ elementary.null_string() }} as column_name,
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
        from
            union_metrics
        where cast(metric_value as {{ dbt_utils.type_int() }}) < {{ var('max_int') }}

    )

    select * from metrics_final

{% endmacro %}