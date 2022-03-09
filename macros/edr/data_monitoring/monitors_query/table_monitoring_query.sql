{% macro table_monitoring_query(monitored_table) %}

-- depends_on: {{ ref('elementary_runs') }}
-- depends_on: {{ ref('final_tables_config') }}
-- depends_on: {{ ref('final_columns_config') }}
-- depends_on: {{ ref('final_should_backfill') }}

{%- set timeframe_end = "'"~ run_started_at.strftime("%Y-%m-%d 00:00:00")~"'" %}
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
         , date_trunc(day, {{ timestamp_column }}) as edr_bucket
        {%- else %}
         , null as edr_bucket
        {%- endif %}
    from {{ full_table_name }}
        where
    {% if is_timestamp -%}
        {{ elementary.cast_to_timestamp(timestamp_column) }} >= {{ elementary.cast_to_timestamp(timeframe_start) }}
        and {{ elementary.cast_to_timestamp(timestamp_column) }} <= {{ elementary.cast_to_timestamp(timeframe_end) }}
    {%- else %}
        true
    {%- endif %}

),

daily_buckets as (

    with dates as
       (select cast({{ timeframe_start }} as timestamp_ntz) as date
            union all
        select {{ dbt_utils.dateadd('day', '1', 'date') }}
        from dates
        where {{ dbt_utils.dateadd('day', '1', 'date') }} < cast({{ timeframe_end }} as timestamp_ntz)
        )
    select date as edr_daily_bucket
    from dates

),

table_monitors as (

    {%- if 'row_count' in table_monitors %}
        select edr_daily_bucket, edr_bucket,
            {%- if 'row_count' in table_monitors %} case when edr_bucket is null then 0 else {{ elementary.row_count() }} end {%- else -%} null {% endif %} as row_count
        from daily_buckets left join timeframe_data on (edr_daily_bucket = edr_bucket)
        group by 1,2
    {%- else %}
        {{ elementary.empty_table([('edr_daily_bucket','timestamp'),('edr_bucket','timestamp'),('row_count','int')]) }}
    {%- endif %}

),

table_monitors_unpivot as (

    {% for monitor in table_monitors_list %}
        select edr_daily_bucket as edr_bucket, '{{ monitor }}' as metric_name, {{ monitor }} as metric_value from table_monitors where {{ monitor }} is not null
        {% if not loop.last %} union all {% endif %}
    {%- endfor %}

),

table_freshness as (

    {%- if 'freshness' in table_monitors and is_timestamp %}
        select
            edr_daily_bucket as edr_bucket,
            'freshness' as metric_name,
            {{ elementary.timediff('minute', 'max('~timestamp_column~')', dbt_utils.dateadd('day','1','edr_daily_bucket')) }} as metric_value
        from daily_buckets, {{ full_table_name }}
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
        null as column_name,
        metric_name,
        metric_value,
        {%- if timestamp_column %}
        edr_bucket as timeframe_start,
        {{ dbt_utils.dateadd('day',1,'edr_bucket') }} as timeframe_end,
        '24' as timeframe_duration_hours
        {%- else %}
        null as timeframe_start,
        null as timeframe_end,
        null as timeframe_duration_hours
        {%- endif %}
    from
        union_metrics
    where cast(metric_value as {{ dbt_utils.type_int() }}) < {{ var('max_int') }}

)

select * from metrics_final


{% endmacro %}