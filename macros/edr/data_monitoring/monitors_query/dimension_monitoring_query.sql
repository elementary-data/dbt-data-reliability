{% macro dimension_monitoring_query(monitored_table_relation, dimensions, timestamp_column, is_timestamp, min_bucket_start) %}

    {% set metric_name = 'dimension' %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set max_bucket_start = "'"~ (elementary.get_run_started_at() - modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
    {% set full_table_name_str = "'"~ elementary.relation_to_full_name(monitored_table_relation) ~"'" %}
    {% set dimensions_sql_expression = elementary.join_list(dimensions, ', ') %}
    {% set concat_dimensions_sql_expression = elementary.list_concat_with_separator(dimensions, ', ') %}
    
    {% if is_timestamp %}
        with all_dimension_values as (
            select distinct dimension_value, 1 as joiner
            from {{ ref('data_monitoring_metrics') }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ "'" ~ metric_name ~ "'" }}
                and dimension = {{ "'" ~ dimensions_sql_expression ~ "'" }}
                and {{ elementary.cast_as_timestamp('bucket_end') }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
        ),

        filtered_monitored_table as (
            select *,
                   {{ concat_dimensions_sql_expression }} as dimension_value,
                   {{ elementary.date_trunc('day', timestamp_column) }} as start_bucket_in_data
            from {{ monitored_table_relation }}
            where
                {{ elementary.cast_as_timestamp(timestamp_column) }} >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
                and {{ elementary.cast_as_timestamp(timestamp_column) }} <= {{ elementary.cast_as_timestamp(max_bucket_end) }}
        ),

        daily_buckets as (
        select 
            edr_daily_bucket,
            1 as joiner
            from (
                {{ elementary.daily_buckets_cte() }}
                where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_start) }} and
                      edr_daily_bucket <= {{ elementary.cast_as_timestamp(max_bucket_start) }} and
                      edr_daily_bucket >= (select min(start_bucket_in_data) from filtered_monitored_table)
            )
        ),

        {# Created daily buckets for each dimension value #}
        dimensions_daily_buckets as (
            select edr_daily_bucket, dimension_value
            from daily_buckets left join all_dimension_values on daily_buckets.joiner = all_dimension_values.joiner
        ),

        {# Calculating the row count for each dimension value's #}
        daily_row_count as (
            select 
                start_bucket_in_data,
                {{ concat_dimensions_sql_expression }} as dimension_value, 
                {{ elementary.cast_as_float(elementary.row_count()) }} as row_count_value
            from filtered_monitored_table
            {{ dbt_utils.group_by(2) }}
        ),

        {# Merging between the daily row count and the dimensions daily buckets #}
        {# This way we make sure that if a dimension has no rows in a day, it will get a metric with value 0 #}
        hydrated_daily_row_count as (
            select edr_daily_bucket,
                   start_bucket_in_data,
                   dimensions_daily_buckets.dimension_value,
                   case when start_bucket_in_data is null then
                       0
                   else row_count_value end as row_count_value
            from dimensions_daily_buckets left join daily_row_count on (edr_daily_bucket = start_bucket_in_data and dimensions_daily_buckets.dimension_value = daily_row_count.dimension_value)
        ),

        row_count as (
            select edr_daily_bucket as edr_bucket,
                   {{ elementary.const_as_string(metric_name) }} as metric_name,
                   {{ elementary.null_string() }} as source_value,
                   row_count_value as metric_value,
                   {{ "'" ~ dimensions_sql_expression ~ "'" }} as dimension,
                   dimension_value
            from hydrated_daily_row_count
        ),

        metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            dimension,
            dimension_value,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket as bucket_start,
            {{ elementary.timeadd('day',1,'edr_bucket') }} as bucket_end,
            24 as bucket_duration_hours
        from
            row_count
        where (metric_value is not null and cast(metric_value as {{ dbt_utils.type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )

    {% else %}
        with row_count as (
            select
                {{ dimensions_sql_expression }},
                {{ elementary.const_as_string(metric_name) }} as metric_name,
                {{ elementary.row_count() }} as metric_value,
                {{ elementary.null_string() }} as source_value,
                {{ "'" ~ dimensions_sql_expression ~ "'" }} as dimension,
                {{ concat_dimensions_sql_expression }} as dimension_value
            from {{ monitored_table_relation }}
            {{ dbt_utils.group_by(2 + dimension | length) }}
        ),

        metrics_final as (
            select
                {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
                {{ elementary.null_string() }} as column_name,
                dimension,
                dimension_value,
                metric_name,
                {{ elementary.cast_as_float('metric_value') }} as metric_value,
                source_value,
                {{ elementary.null_timestamp() }} as bucket_start,
                {{ elementary.cast_as_timestamp(max_bucket_end) }} as bucket_end,
                {{ elementary.null_int() }} as bucket_duration_hours
            from row_count
        )
    {% endif %}

    select
        {{ dbt_utils.surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'dimension',
            'dimension_value',
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
        {{- dbt_utils.current_timestamp_in_utc() -}} as updated_at,
        dimension,
        dimension_value
    from metrics_final

{% endmacro %}