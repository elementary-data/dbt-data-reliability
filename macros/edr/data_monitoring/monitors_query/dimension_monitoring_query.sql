{% macro dimension_monitoring_query(monitored_table_relation, dimensions, min_bucket_start, max_bucket_end, days_back, metric_properties) %}
    {% set metric_name = 'dimension' %}
    {% set full_table_name_str = elementary.edr_quote(elementary.relation_to_full_name(monitored_table_relation)) %}
    {% set dimensions_string = elementary.join_list(dimensions, '; ') %}
    {% set concat_dimensions_sql_expression = elementary.list_concat_with_separator(dimensions, '; ') %}

    {% set timestamp_column = metric_properties.timestamp_column %}

    {% if timestamp_column %}
        with buckets as (
          select
            edr_bucket_start,
            edr_bucket_end,
            1 as joiner
          from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) }}) results
          where edr_bucket_start >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
            and edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
        ),

        filtered_monitored_table as (
            select *,
                   {{ concat_dimensions_sql_expression }} as dimension_value,
                   {{ elementary.get_start_bucket_in_data(timestamp_column, min_bucket_start, metric_properties.time_bucket) }} as start_bucket_in_data
            from {{ monitored_table_relation }}
            where
                {{ elementary.edr_cast_as_timestamp(timestamp_column) }} >= (select min(edr_bucket_start) from buckets)
                and {{ elementary.edr_cast_as_timestamp(timestamp_column) }} < (select max(edr_bucket_end) from buckets)
            {% if metric_properties.where_expression %}
                and {{ metric_properties.where_expression }}
            {% endif %}
        ),

        {# Outdated dimension values are dimensions with all metrics of 0 in the range of the test time #}
        dimension_values_without_outdated as (
            select distinct 
                dimension_value,
                sum(metric_value)
            from {{ ref('data_monitoring_metrics') }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ elementary.edr_quote(metric_name) }}
                and dimension = {{ elementary.edr_quote(dimensions_string) }}
                and {{ elementary.edr_cast_as_timestamp('bucket_end') }} >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
            group by 1
            having sum(metric_value) > 0
        ),

        dimension_values_union as (
            select distinct *
            from (
                select distinct 
                    dimension_value,
                    1 as joiner
                from dimension_values_without_outdated
                union all
                select distinct
                    dimension_value,
                    1 as joiner
                from filtered_monitored_table
            ) results
        ),

        {# Created buckets for each dimension value #}
        dimensions_buckets as (
            select edr_bucket_start, edr_bucket_end, dimension_value
            from buckets left join dimension_values_union on buckets.joiner = dimension_values_union.joiner
        ),

        {# Calculating the row count for the value of each dimension #}
        filtered_row_count_values as (
            select 
                start_bucket_in_data,
                {{ concat_dimensions_sql_expression }} as dimension_value, 
                {{ elementary.edr_cast_as_float(elementary.row_count()) }} as row_count_value
            from filtered_monitored_table
            {{ dbt_utils.group_by(2) }}
        ),

        {# Merging between the row count and the dimensions buckets #}
        {# This way we make sure that if a dimension has no rows in a day, it will get a metric with value 0 #}
        row_count_values as (
            select edr_bucket_start,
                   edr_bucket_end,
                   start_bucket_in_data,
                   dimensions_buckets.dimension_value,
                   case when start_bucket_in_data is null then
                       0
                   else row_count_value end as row_count_value
            from dimensions_buckets left join filtered_row_count_values on (edr_bucket_start = start_bucket_in_data and dimensions_buckets.dimension_value = filtered_row_count_values.dimension_value)
        ),

        row_count as (
            select edr_bucket_start,
                   edr_bucket_end,
                   {{ elementary.const_as_string(metric_name) }} as metric_name,
                   {{ elementary.null_string() }} as source_value,
                   row_count_value as metric_value,
                   {{ elementary.const_as_string(dimensions_string) }} as dimension,
                   dimension_value,
                   {{elementary.dict_to_quoted_json(metric_properties) }} as metric_properties
            from row_count_values
        ),

        metrics_final as (

        select
            {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket_start as bucket_start,
            edr_bucket_end as bucket_end,
            {{ elementary.timediff("hour", "edr_bucket_start", "edr_bucket_end") }} as bucket_duration_hours,
            dimension,
            dimension_value,
            metric_properties
        from
            row_count
        where (metric_value is not null and cast(metric_value as {{ elementary.edr_type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )

    {% else %}
        with filtered_monitored_table as (
            select *,
                   {{ concat_dimensions_sql_expression }} as dimension_value
            from {{ monitored_table_relation }}
        {% if metric_properties.where_expression %}
            where {{ metric_properties.where_expression }}
        {% endif %}
        ),
        
        {# Get all of the dimension anomally metrics that were created for the test until this run #}
        last_dimension_metrics as (
            select 
                bucket_end,
                dimension_value,
                metric_value
            from {{ ref('data_monitoring_metrics') }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ elementary.edr_quote(metric_name) }}
                and dimension = {{ elementary.edr_quote(dimensions_string) }}
                and {{ elementary.edr_cast_as_timestamp('bucket_end') }} >= {{ elementary.edr_timeadd(metric_properties.time_bucket.period,
                                                                                              metric_properties.time_bucket.count,
                                                                                              elementary.edr_cast_as_timestamp(min_bucket_start)) }}
        ),

        {# Outdated dimension values are dimensions with all metrics of 0 in the range of the test time #}
        dimension_values_without_outdated as (
            select
                bucket_end,
                dimension_value,
                metric_value
            from last_dimension_metrics
            where dimension_value in (
                select dimension_value
                from (
                    select distinct 
                        dimension_value,
                        sum(metric_value)
                    from last_dimension_metrics
                    group by 1
                    having sum(metric_value) > 0
                ) results
            )
        ),
        

        dimension_values_union as (
            select distinct *
            from (
                select distinct 
                    dimension_value,
                    1 as joiner
                from dimension_values_without_outdated
                union all
                select distinct 
                    dimension_value,
                    1 as joiner
                from filtered_monitored_table
            ) results
        ),

        {# Create buckets for each day from max(first metric time, min bucket end) until max bucket end #}
        buckets as (
          select
            edr_bucket_start,
            edr_bucket_end,
            1 as joiner
          from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) }}) results
          where edr_bucket_start >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
            and edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
        ),

        {# Get all of the metrics for all of the dimensions that were create for the test until this run, #}
        {# "hydrated" with metrics with value 0 for dimensions with no row count in the given time range. #}
        hydrated_last_dimension_metrics as (
            select 
                edr_bucket_end as bucket_end,
                dimension_values_union.dimension_value as dimension_value,
                case when metric_value is not null then metric_value else 0 end as metric_value
            from buckets left join dimension_values_union on buckets.joiner = dimension_values_union.joiner
                left outer join dimension_values_without_outdated on (buckets.edr_bucket_end = dimension_values_without_outdated.bucket_end and dimension_values_union.dimension_value = dimension_values_without_outdated.dimension_value)
        ),

        {# Union between current row count for each dimension, and the "hydrated" metrics of the test until this run #}
        row_count as (
            select 
                bucket_end,
                dimension_value,
                metric_value
            from hydrated_last_dimension_metrics
            union all
            select
                {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }} as bucket_end,
                {{ concat_dimensions_sql_expression }} as dimension_value,
                {{ elementary.row_count() }} as metric_value
            from {{ monitored_table_relation }}
            {% if metric_properties.where_expression %}
                where {{ metric_properties.where_expression }}
            {% endif %}
            {{ dbt_utils.group_by(2) }}
        ),

        metrics_final as (
            select
                {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
                {{ elementary.null_string() }} as column_name,
                {{ elementary.const_as_string(metric_name) }} as metric_name,
                {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
                {{ elementary.null_string() }} as source_value,
                {{ elementary.null_timestamp() }} as bucket_start,
                bucket_end,
                {{ elementary.null_int() }} as bucket_duration_hours,
                {{ elementary.const_as_string(dimensions_string) }} as dimension,
                dimension_value,
                {{ elementary.dict_to_quoted_json(metric_properties) }} as metric_properties
            from row_count
        )
    {% endif %}

    select
        {{ elementary.generate_surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'dimension',
            'dimension_value',
            'bucket_end',
            'metric_properties']) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_value,
        source_value,
        bucket_start,
        bucket_end,
        bucket_duration_hours,
        {{ elementary.edr_current_timestamp_in_utc() }} as updated_at,
        dimension,
        dimension_value,
        metric_properties
    from metrics_final

{% endmacro %}