{% macro dimension_monitoring_query(monitored_table_relation, dimensions, timestamp_column, is_timestamp, min_bucket_start) %}
    {% set metric_name = 'dimension' %}
    {%- set max_bucket_end = "'"~ elementary.get_run_started_at().strftime("%Y-%m-%d 00:00:00")~"'" %}
    {%- set min_bucket_end = "'"~ (modules.datetime.datetime.strptime(min_bucket_start, "'%Y-%m-%d %H:%M:%S'") +  modules.datetime.timedelta(1)).strftime("%Y-%m-%d 00:00:00")~"'" %}
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
        {# Get all of the dimension anomally metrics that were created for the test until this run #}
        with last_dimension_metrics as (
            select 
                bucket_end,
                dimension_value,
                metric_value
            from {{ ref('data_monitoring_metrics') }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ "'" ~ metric_name ~ "'" }}
                and dimension = {{ "'" ~ dimensions_sql_expression ~ "'" }}
                and {{ elementary.cast_as_timestamp('bucket_end') }} >= {{ elementary.cast_as_timestamp(min_bucket_end) }}
                and {{ elementary.cast_as_timestamp('bucket_end') }} < {{ elementary.cast_as_timestamp(max_bucket_end) }}
        ),
        

        all_dimension_values as (
            select distinct 
                dimension_value,
                1 as joiner
            from last_dimension_metrics
        ),

        {# Create buckets for each day from max(fisrt metric time, min bucket end) until max bucket end #}
        daily_buckets as (
        select 
            edr_daily_bucket,
            1 as joiner
            from (
                {{ elementary.daily_buckets_cte() }}
                where edr_daily_bucket >= {{ elementary.cast_as_timestamp(min_bucket_end) }} and
                      edr_daily_bucket < {{ elementary.cast_as_timestamp(max_bucket_end) }} and
                      edr_daily_bucket >= (select min(bucket_end) from last_dimension_metrics)
            )
        ),

        {# Get all of the metrics for all of the dimensions that were create for the test until this run, #}
        {# "hydrated" with metrics with value 0 for dimensions with no row count in the given time range. #}
        hydrated_last_dimension_metrics as (
            select 
                edr_daily_bucket as bucket_end,
                all_dimension_values.dimension_value as dimension_value,
                case when metric_value is not null then metric_value else 0 end as metric_value
            from daily_buckets left join all_dimension_values on daily_buckets.joiner = all_dimension_values.joiner
                left outer join last_dimension_metrics on (daily_buckets.edr_daily_bucket = last_dimension_metrics.bucket_end and all_dimension_values.dimension_value = last_dimension_metrics.dimension_value)
        ),

        {# Union between current roe count for each dimension, and the "hydrated" metrics of the test until this run #}
        row_count as (
            select 
                bucket_end,
                dimension_value,
                metric_value
            from hydrated_last_dimension_metrics
            union all
            select
                {{ elementary.cast_as_timestamp(max_bucket_end) }} as bucket_end,
                {{ concat_dimensions_sql_expression }} as dimension_value,
                {{ elementary.row_count() }} as metric_value
            from {{ monitored_table_relation }}
            {{ dbt_utils.group_by(2) }}
        ),

        metrics_final as (
            select
                {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
                {{ elementary.null_string() }} as column_name,
                {{ "'" ~ dimensions_sql_expression ~ "'" }} as dimension,
                dimension_value,
                {{ elementary.const_as_string(metric_name) }} as metric_name,
                {{ elementary.cast_as_float('metric_value') }} as metric_value,
                {{ elementary.null_string() }} as source_value,
                {{ elementary.null_timestamp() }} as bucket_start,
                bucket_end,
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