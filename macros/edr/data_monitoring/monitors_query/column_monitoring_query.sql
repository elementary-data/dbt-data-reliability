{% macro column_monitoring_query(monitored_table_relation, timestamp_column, min_bucket_start, column_obj, column_monitors, where_expression=none, time_bucket=none) %}

    {% set full_table_name_str = elementary.quote(elementary.relation_to_full_name(monitored_table_relation)) %}

    with buckets as (
        select edr_bucket_start, edr_bucket_end from ({{ elementary.complete_buckets_cte(time_bucket) }})
        {% if min_bucket_start -%}
          where edr_bucket_start >= {{ elementary.cast_as_timestamp(min_bucket_start) }}
        {%- endif %}
    ),

    filtered_monitored_table as (
        select {{ column_obj.quoted }}
            {% if timestamp_column -%}
             , {{ elementary.get_start_bucket_in_data(timestamp_column, min_bucket_start, time_bucket) }} as start_bucket_in_data
            {%- else %}
            , {{ elementary.null_timestamp() }} as start_bucket_in_data
            {%- endif %}
        from {{ monitored_table_relation }}
        where
        {% if timestamp_column -%}
            {{ elementary.cast_as_timestamp(timestamp_column) }} >= (select min(edr_bucket_start) from buckets)
            and {{ elementary.cast_as_timestamp(timestamp_column) }} < (select max(edr_bucket_end) from buckets)
        {%- else %}
            true
        {%- endif %}
        {% if where_expression %} and {{ where_expression }} {% endif %}
    ),

    column_monitors as (

        {%- if column_monitors %}
            {%- set column = column_obj.quoted -%}
                select
                    edr_bucket_start,
                    edr_bucket_end,
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
                    {%- if 'missing_percent' in column_monitors -%} {{ elementary.missing_percent(column) }} {%- else -%} null {% endif %} as missing_percent
                from filtered_monitored_table left join buckets on (edr_bucket_start = start_bucket_in_data)
                group by 1,2,3
        {%- else %}
            {{ elementary.empty_column_monitors_cte() }}
        {%- endif %}

    ),

    column_monitors_unpivot as (

        {%- if column_monitors %}
            {% for monitor in column_monitors %}
                select edr_column_name, edr_bucket_start, edr_bucket_end, {{ elementary.cast_as_string(elementary.quote(monitor)) }} as metric_name, {{ elementary.cast_as_float(monitor) }} as metric_value from column_monitors where {{ monitor }} is not null
                {% if not loop.last %} union all {% endif %}
            {%- endfor %}
        {%- else %}
            {{ elementary.empty_table([('edr_column_name','string'),('edr_bucket_start','timestamp'),('edr_bucket_end','timestamp'),('metric_name','string'),('metric_value','float')]) }}
        {%- endif %}

    ),

    metrics_final as (

        select
            {{ elementary.cast_as_string(full_table_name_str) }} as full_table_name,
            edr_column_name as column_name,
            metric_name,
            {{ elementary.cast_as_float('metric_value') }} as metric_value,
            {{ elementary.null_string() }} as source_value,
            {%- if timestamp_column %}
                edr_bucket_start as bucket_start,
                edr_bucket_end as bucket_end,
                {{ elementary.datediff("edr_bucket_start", "edr_bucket_end", "hour") }} as bucket_duration_hours,
            {%- else %}
                {{ elementary.null_timestamp() }} as bucket_start,
                {{ elementary.cast_as_timestamp(elementary.quote(elementary.get_max_bucket_end())) }} as bucket_end,
                {{ elementary.null_int() }} as bucket_duration_hours,
            {%- endif %}
            {{ elementary.null_string() }} as dimension,
            {{ elementary.null_string() }} as dimension_value
        from column_monitors_unpivot

    )

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