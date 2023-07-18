{% macro column_monitoring_query(monitored_table, monitored_table_relation, min_bucket_start, max_bucket_end, days_back, column_obj, column_monitors, metric_properties) %}
    {%- set full_table_name_str = elementary.edr_quote(elementary.relation_to_full_name(monitored_table_relation)) %}
    {%- set timestamp_column = metric_properties.timestamp_column %}


    with monitored_table as (
        select * from {{ monitored_table }}
            {% if metric_properties.where_expression %} where {{ metric_properties.where_expression }} {% endif %}
    ),

    {% if timestamp_column -%}
         buckets as (
             select edr_bucket_start, edr_bucket_end
             from ({{ elementary.complete_buckets_cte(metric_properties, min_bucket_start, max_bucket_end) }}) results
             where edr_bucket_start >= {{ elementary.edr_cast_as_timestamp(min_bucket_start) }}
               and edr_bucket_end <= {{ elementary.edr_cast_as_timestamp(max_bucket_end) }}
         ),

         filtered_monitored_table as (
            select {{ column_obj.quoted }},
                   {{ elementary.get_start_bucket_in_data(timestamp_column, min_bucket_start, metric_properties.time_bucket) }} as start_bucket_in_data
            from monitored_table
            where
                {{ elementary.edr_cast_as_timestamp(timestamp_column) }} >= (select min(edr_bucket_start) from buckets)
                and {{ elementary.edr_cast_as_timestamp(timestamp_column) }} < (select max(edr_bucket_end) from buckets)
        ),
    {%- else %}
        filtered_monitored_table as (
            select {{ column_obj.quoted }},
                   {{ elementary.null_timestamp() }} as start_bucket_in_data
            from monitored_table
        ),
    {% endif %}

    column_monitors as (

        {%- if column_monitors %}
            {%- set column = column_obj.quoted -%}
                select
                    {%- if timestamp_column %}
                        edr_bucket_start as bucket_start,
                        edr_bucket_end as bucket_end,
                        {{ elementary.timediff("hour", "edr_bucket_start", "edr_bucket_end") }} as bucket_duration_hours,
                    {%- else %}
                        {{ elementary.null_timestamp() }} as bucket_start,
                        {{ elementary.edr_cast_as_timestamp(elementary.edr_quote(elementary.run_started_at_as_string())) }} as bucket_end,
                        {{ elementary.null_int() }} as bucket_duration_hours,
                    {%- endif %}
                    {{ elementary.const_as_string(column_obj.name) }} as edr_column_name,
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
                    {%- if 'missing_percent' in column_monitors -%} {{ elementary.missing_percent(column) }} {%- else -%} null {% endif %} as missing_percent,
                    {%- if 'sum' in column_monitors -%} {{ elementary.sum(column) }} {%- else -%} null {% endif %} as sum
                from filtered_monitored_table
                {%- if timestamp_column %}
                    left join buckets on (edr_bucket_start = start_bucket_in_data)
                {%- endif %}
                group by 1,2,3,4
        {%- else %}
            {{ elementary.empty_column_monitors_cte() }}
        {%- endif %}

    ),

    column_monitors_unpivot as (

        {%- if column_monitors %}
            {% for monitor in column_monitors %}
                select edr_column_name, bucket_start, bucket_end, bucket_duration_hours, {{ elementary.edr_cast_as_string(elementary.edr_quote(monitor)) }} as metric_name, {{ elementary.edr_cast_as_float(monitor) }} as metric_value from column_monitors where {{ monitor }} is not null
                {% if not loop.last %} union all {% endif %}
            {%- endfor %}
        {%- else %}
            {{ elementary.empty_table([('edr_column_name','string'),('bucket_start','timestamp'),('bucket_end','timestamp'),('bucket_duration_hours','int'),('metric_name','string'),('metric_value','float')]) }}
        {%- endif %}

    ),

    metrics_final as (

        select
            {{ elementary.edr_cast_as_string(full_table_name_str) }} as full_table_name,
            edr_column_name as column_name,
            metric_name,
            {{ elementary.edr_cast_as_float('metric_value') }} as metric_value,
            {{ elementary.null_string() }} as source_value,
            bucket_start,
            bucket_end,
            bucket_duration_hours,
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value,
            {{elementary.dict_to_quoted_json(metric_properties) }} as metric_properties
        from column_monitors_unpivot

    )

    select
        {{ elementary.generate_surrogate_key([
            'full_table_name',
            'column_name',
            'metric_name',
            'bucket_end',
            'metric_properties'
        ]) }} as id,
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
