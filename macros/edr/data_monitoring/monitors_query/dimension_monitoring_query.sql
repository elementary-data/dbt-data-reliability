{% macro dimension_monitoring_query(monitored_table_relation, dimension, timestamp_column, is_timestamp, min_bucket_start) %}

    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set full_table_name_str = "'"~ elementary.relation_to_full_name(monitored_table_relation) ~"'" %}
    {% set dimension_str = elementary.join_list(dimension, ', ') %}
    {% set concat_dimension_query = elementary.join_list(dimension, ' || ') %}
    {% set list_concat_dimension_query = elementary.list_concat_with_separator(dimension, '; ') %}
    
    {{ debug() }}
    
    {% if is_timestamp %}
        with filtered_monitored_table as (
            select *,
                   {{ elementary.date_trunc('day', timestamp_column) }} as start_bucket_in_data
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

        daily_row_count as (
            select edr_daily_bucket,
                   start_bucket_in_data,
                   {{ dimension_str }},
                   case when start_bucket_in_data is null then
                       0
                   else {{ elementary.cast_as_float(elementary.row_count()) }} end as row_count_value
            from daily_buckets left join filtered_monitored_table on (edr_daily_bucket = start_bucket_in_data)
            {# group by 1,2 #}
            {{ dbt_utils.group_by(2 + dimension | length) }}
        ),

        row_count as (
            select edr_daily_bucket as edr_bucket,
                   {{ elementary.const_as_string('dimension_row_count') }} as metric_name,
                   {{ elementary.null_string() }} as source_value,
                   row_count_value as metric_value,
                   {{ list_concat_dimension_query }} as dimension_value
            from daily_row_count
        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ "'" ~ dimension_str ~ "'" }} as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket as bucket_start,
            {{ elementary.timeadd('day',1,'edr_bucket') }} as bucket_end,
            24 as bucket_duration_hours,
            dimension_value
        from
            row_count
        where (metric_value is not null and cast(metric_value as {{ dbt_utils.type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )

    {% else %}
        with row_count as (
            select
                {{ dimension_str }},
                {{ elementary.const_as_string('dimension_row_count') }} as metric_name,
                {{ elementary.row_count() }} as metric_value,
                {{ list_concat_dimension_query }} as dimension_value
            from {{ monitored_table_relation }}
            {{ dbt_utils.group_by(1 + dimension | length) }}
        ),

        metrics_final as (
            select
                {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
                {{ "'" ~ dimension_str ~ "'" }} as column_name,
                metric_name,
                {{ elementary.cast_as_float('metric_value') }} as metric_value,
                {{ elementary.null_string() }} as source_value,
                {{ elementary.null_timestamp() }} as bucket_start,
                {{ elementary.cast_as_timestamp(max_bucket_end) }} as bucket_end,
                {{ elementary.null_int() }} as bucket_duration_hours,
                dimension_value
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
        dimension_value,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{- dbt_utils.current_timestamp_in_utc() -}} as updated_at
    from metrics_final

{% endmacro %}