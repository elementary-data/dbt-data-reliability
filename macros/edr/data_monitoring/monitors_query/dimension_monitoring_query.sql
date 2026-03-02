{% macro dimension_monitoring_query(monitored_table, monitored_table_relation, dimensions, min_bucket_start, max_bucket_end, metric_properties, metric_name=none) %}
    {% set metric_name = metric_name or 'dimension' %}
    {% set full_table_name_str = elementary.edr_quote(elementary.relation_to_full_name(monitored_table_relation)) %}
    {% set dimensions_string = elementary.join_list(dimensions, '; ') %}
    {% set concat_dimensions_sql_expression = elementary.list_concat_with_separator(dimensions, '; ') %}
    {% set timestamp_column = metric_properties.timestamp_column %}
    {%- set data_monitoring_metrics_relation = elementary.get_elementary_relation('data_monitoring_metrics') %}

    with filtered_monitored_table as (
        select *,
        {{ concat_dimensions_sql_expression }} as dimension_value
        from {{ monitored_table }}
        {% if metric_properties.where_expression %}
            where {{ metric_properties.where_expression }}
        {% endif %}
    ),

    {% if timestamp_column %}
        buckets as (
          select
            edr_bucket_start,
            edr_bucket_end,
            1 as joiner
          from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) }}) results
          where edr_bucket_start >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
            and edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
        ),

        time_filtered_monitored_table as (
            select *,
                   {{ elementary.get_start_bucket_in_data(timestamp_column, min_bucket_start, metric_properties.time_bucket) }} as start_bucket_in_data
            from filtered_monitored_table
            where
                {{ elementary.edr_cast_as_timestamp(timestamp_column) }} >= (select min(edr_bucket_start) from buckets)
                and {{ elementary.edr_cast_as_timestamp(timestamp_column) }} < (select max(edr_bucket_end) from buckets)
        ),

        all_dimension_metrics as (
            select
                bucket_end,
                dimension_value,
                metric_value
            from {{ data_monitoring_metrics_relation }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ elementary.edr_quote(metric_name) }}
                and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
        ),

        training_set_dimensions as (
            select distinct
                dimension_value,
                1 as joiner,
                min(bucket_end) as dimension_min_bucket_end,
                sum(metric_value)
            from all_dimension_metrics
            group by 1,2
            {# Remove outdated dimension values (dimensions with all metrics of 0 in the range of the test time) #}
            having sum(metric_value) > 0
        ),

        {# Create buckets for each previous dimension value #}
        dimensions_buckets as (
            select edr_bucket_start, edr_bucket_end, dimension_value
            from training_set_dimensions left join buckets
                on (buckets.joiner = training_set_dimensions.joiner
                {# This makes sure we dont create empty buckets for dimensions before their first appearance #}
                and edr_bucket_end >= dimension_min_bucket_end)
            where dimension_value is not null
        ),

        {# Calculating the row count for the value of each dimension #}
        row_count_values as (
            select
                edr_bucket_start,
                edr_bucket_end,
                start_bucket_in_data,
                dimension_value,
                case when start_bucket_in_data is null then
                    0
                else {{ elementary.edr_cast_as_float(elementary.row_count()) }} end as row_count_value
            from buckets left join time_filtered_monitored_table on (edr_bucket_start = start_bucket_in_data)
            group by 1,2,3,4
        ),

        {# Merging between the row count and the dimensions buckets #}
        {# This way we make sure that if a dimension has no rows, it will get a metric with value 0 #}
        fill_empty_buckets_row_count_values as (
            select dimensions_buckets.edr_bucket_start,
                   dimensions_buckets.edr_bucket_end,
                   start_bucket_in_data,
                   dimensions_buckets.dimension_value,
                   case when start_bucket_in_data is null then
                       0
                   else row_count_value end as row_count_value
            from dimensions_buckets left join row_count_values
                on (dimensions_buckets.edr_bucket_start = start_bucket_in_data and dimensions_buckets.dimension_value = row_count_values.dimension_value)
        ),

        {# We union so new buckets added in this run will be included (were filtered by the join we did on 'fill_empty_buckets_row_count_values') #}
        union_row_count_values as (
            select distinct *
            from
            (
            select * from row_count_values
              union all
            select * from fill_empty_buckets_row_count_values
            ) as results
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
            from union_row_count_values
        ),

        metrics_final as (

        select
            {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
            {{ elementary.null_string() }} as column_name,
            metric_name,
            {{ elementary.const_as_string("row_count") }} as metric_type,
            {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
            source_value,
            edr_bucket_start as bucket_start,
            edr_bucket_end as bucket_end,
            {{ elementary.timediff("hour", "edr_bucket_start", "edr_bucket_end") }} as bucket_duration_hours,
            dimension,
            dimension_value,
            metric_properties
        from row_count
        where (metric_value is not null and cast(metric_value as {{ elementary.edr_type_int() }}) < {{ elementary.get_config_var('max_int') }}) or
            metric_value is null
        )

    {% else %}

        {# Get all of the dimension anomaly metrics that were created for the test until this run #}
        all_dimension_metrics as (
            select
                bucket_end,
                dimension_value,
                metric_value
            from {{ data_monitoring_metrics_relation }}
            where full_table_name = {{ full_table_name_str }}
                and metric_name = {{ elementary.edr_quote(metric_name) }}
                and metric_properties = {{ elementary.dict_to_quoted_json(metric_properties) }}
        ),

        training_set_dimensions as (
            select distinct
                dimension_value,
                sum(metric_value)
            from all_dimension_metrics
            group by 1
            {# Remove outdated dimension values (dimensions with all metrics of 0 in the range of the test time) #}
            having sum(metric_value) > 0
        ),

        {# Calculating the row count for the value of each dimension #}
        row_count_values as (
            select
                {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }} as bucket_end,
                dimension_value,
                {{ elementary.edr_cast_as_float(elementary.row_count()) }} as row_count_value
            from filtered_monitored_table
            group by 1,2
        ),

        {# This way we make sure that if a dimension has no rows, it will get a metric with value 0 #}
        fill_empty_dimensions_row_count_values as (
            select {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }} as bucket_end,
                   dimension_value,
                   0 as row_count_value
            from training_set_dimensions
            where dimension_value not in (select distinct dimension_value from row_count_values)
        ),

        {# Union between current row count for each dimension, and the "hydrated" metrics of the test until this run #}
        row_count as (
            select * from row_count_values
            union all
            select * from fill_empty_dimensions_row_count_values
        ),

        metrics_final as (
            select
                {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
                {{ elementary.null_string() }} as column_name,
                {{ elementary.const_as_string(metric_name) }} as metric_name,
                {{ elementary.const_as_string("row_count") }} as metric_type,
                {{ elementary.edr_cast_as_float('row_count_value') }} as metric_value,
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
            'metric_type',
            'dimension',
            'dimension_value',
            'bucket_end',
            'metric_properties']) }} as id,
        full_table_name,
        column_name,
        metric_name,
        metric_type,
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