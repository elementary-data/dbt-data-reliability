{% macro table_monitoring_query(monitored_table_relation, timestamp_column, is_timestamp, min_bucket_start, table_monitors, freshness_column=none) %}

    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set full_table_name_str = "'"~ elementary.relation_to_full_name(monitored_table_relation) ~"'" %}

    {% if is_timestamp %}
        with filtered_monitored_table as (
            select *,
                   {{ elementary.time_trunc('day', timestamp_column) }} as start_bucket_in_data
            from {{ monitored_table_relation }}
            where
                {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
                and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
        ),

        daily_buckets as (
            {{ elementary.daily_buckets_cte() }}
            where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_start) }} and
                  edr_daily_bucket <= {{ elementary.cast_as_timestamp(max_bucket_start) }} and
                  edr_daily_bucket >= (select min(start_bucket_in_data) from filtered_monitored_table)
        ),

        {%- if 'row_count' in table_monitors %}

        daily_row_count as (
            select edr_daily_bucket,
                   start_bucket_in_data,
                   case when start_bucket_in_data is null then
                       0
                   else {{ elementary.cast_as_float(elementary.row_count()) }} end as row_count_value
            from daily_buckets left join filtered_monitored_table on (edr_daily_bucket = start_bucket_in_data)
            group by 1,2
        ),

        row_count as (
            select edr_daily_bucket as edr_bucket,
                   {{ elementary.const_as_string('row_count') }} as metric_name,
                   {{ elementary.null_string() }} as source_value,
                   row_count_value as metric_value
            from daily_row_count
        ),

        {%- else %}

        row_count as (
            {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','string'),('source_value','string'),('metric_value','int')]) }}
        ),

        {%- endif %}

        table_freshness as (
        {%- if 'freshness' in table_monitors %}
            {%- if freshness_column is undefined or freshness_column is none %}
                {%- set freshness_column = timestamp_column %}
            {%- endif %}
            select
                edr_daily_bucket as edr_bucket,
                {{ elementary.const_as_string('freshness') }} as metric_name,
                {{ elementary.cast_as_string('max('~freshness_column~')') }} as source_value,
                {{ elementary.timediff('second', elementary.cast_as_timestamp('max('~freshness_column~')'), elementary.timeadd('day','1','edr_daily_bucket')) }} as metric_value
            from daily_buckets, {{ monitored_table_relation }}
            where {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.timeadd('day','1','edr_daily_bucket') }}
            group by 1,2
        {%- else %}
            {{ elementary.empty_table([('edr_bucket','timestamp'),('metric_name','string'),('source_value','string'),('metric_value','int')]) }}
        {%- endif %}
        ),

        union_metrics as (

        select * from row_count
        union all
        select * from table_freshness

        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket as bucket_start,
            {{ elementary.timeadd('day',1,'edr_bucket') }} as bucket_end,
            24 as bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value
        from
            union_metrics
        where (metric_value is not null and cast(metric_value as {{ elementary.type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )
    {% else %}
        with row_count as (
            {%- if 'row_count' in table_monitors %}
                select
                    {{ elementary.const_as_string('row_count') }} as metric_name,
                    {{ elementary.row_count() }} as metric_value
                from {{ monitored_table_relation }}
                group by 1
            {%- else %}
                {{ elementary.empty_table([('metric_name','string'),('metric_value','int')]) }}
            {%- endif %}
        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            {{ elementary.null_string() }} as source_value,
            {{ elementary.null_timestamp() }} as bucket_start,
            {{ elementary.cast_as_timestamp(max_bucket_end) }} as bucket_end,
            {{ elementary.null_int() }} as bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value
        from row_count

        )
    {% endif %}

    select
        {{ dbt_utils.surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'bucket_end'
        ]) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{- elementary.current_timestamp_in_utc() -}} as updated_at,
        dimension,
        dimension_value
    from metrics_final

{% endmacro %}