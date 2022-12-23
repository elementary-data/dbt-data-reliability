{% macro table_monitoring_query(monitored_table_relation, timestamp_column, min_bucket_start, table_monitors, freshness_column, where_expression, time_bucket) %}

    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set full_table_name_str = "'"~ elementary.relation_to_full_name(monitored_table_relation) ~"'" %}

    {% set bucket_start_datediff_expr %}
      floor({{ elementary.datediff(min_bucket_start, elementary.cast_as_timestamp(timestamp_column), time_bucket.period) }} / {{ time_bucket.count }}) * {{ time_bucket.count }}
    {% endset %}

    {% if timestamp_column %}
        with filtered_monitored_table as (
            select *,
                   {{ elementary.cast_as_timestamp(elementary.dateadd(time_bucket.period, elementary.cast_as_int(bucket_start_datediff_expr), min_bucket_start)) }} as start_bucket_in_data
            from {{ monitored_table_relation }}
            where
                {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
                and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
                {% if where_expression %} and {{ where_expression }} {% endif %}
        ),

        buckets as (
            {{ elementary.buckets_cte(time_bucket) }}
            where edr_bucket >= {{ elementary.cast_as_timestamp(min_bucket_start) }} and
                  edr_bucket <= {{ elementary.cast_as_timestamp(max_bucket_start) }} and
                  edr_bucket >= (select min(start_bucket_in_data) from filtered_monitored_table)
        ),

        {%- if 'row_count' in table_monitors %}

        row_count_values as (
            select edr_bucket,
                   start_bucket_in_data,
                   case when start_bucket_in_data is null then
                       0
                   else {{ elementary.cast_as_float(elementary.row_count()) }} end as row_count_value
            from buckets left join filtered_monitored_table on (edr_bucket = start_bucket_in_data)
            group by 1,2
        ),

        row_count as (
            select edr_bucket,
                   {{ elementary.const_as_string('row_count') }} as metric_name,
                   {{ elementary.null_string() }} as source_value,
                   row_count_value as metric_value
            from row_count_values
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
                edr_bucket,
                {{ elementary.const_as_string('freshness') }} as metric_name,
                {{ elementary.cast_as_string('max('~freshness_column~')') }} as source_value,
                {{ elementary.timediff('second', elementary.cast_as_timestamp('max('~freshness_column~')'), elementary.timeadd(time_bucket.period, time_bucket.count, 'edr_bucket')) }} as metric_value
            from buckets, {{ monitored_table_relation }}
            where {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.timeadd(time_bucket.period, time_bucket.count, 'edr_bucket') }}
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
            {{ elementary.timeadd(time_bucket.period, time_bucket.count, 'edr_bucket') }} as bucket_end,
            {{ elementary.datediff("edr_bucket", elementary.timeadd(time_bucket.period, time_bucket.count, "edr_bucket"), "hour") }} as bucket_duration_hours,
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
        {{ elementary.generate_surrogate_key([
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
        {{ elementary.current_timestamp_in_utc() }} as updated_at,
        dimension,
        dimension_value
    from metrics_final

{% endmacro %}