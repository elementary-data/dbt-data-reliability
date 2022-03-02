{% macro one_bucket_monitors_query(monitored_table, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) %}

    (
        with timeframe_data as (

            select *
            from {{ monitored_table }}
            where
            {% if timestamp_field and timeframe_start and timeframe_end -%}
                {%- if timestamp_column_data_type == 'datetime' %}
                    {{ timestamp_field }} > {{ timeframe_start }} and {{ timestamp_field }} < {{ timeframe_end }}
                {%- elif timestamp_column_data_type == 'string' %}
                    {{ elementary.cast_string_column_to_timestamp(timestamp_field) }} > {{ timeframe_start }}
                    and {{ elementary.cast_string_column_to_timestamp(timestamp_field) }} < {{ timeframe_end }}
                {%- endif %}
            {%- else -%}
                true
            {%- endif -%}

    ),

    table_monitors as (

        {{- elementary.table_monitors_cte(table_monitors, timestamp_field, timeframe_end, monitored_table) }}

    ),

    column_monitors as (

        {{- elementary.column_monitors_cte(column_config) }}

    ),

    union_metrics as (

        select * from table_monitors
        union all
        select * from column_monitors

    ),

    metrics_final as (

        select
            '{{ monitored_table }}' as full_table_name,
            column_name,
            metric_name,
            metric_value,
            {%- if timeframe_start %}
                {{- timeframe_start }} as timeframe_start,
            {%- else %}
                null as timeframe_start,
            {%- endif %}
            {%- if timeframe_end %}
                {{- timeframe_end }} as timeframe_end,
            {%- else %}
                null as timeframe_end,
            {%- endif %}
            {%- if timeframe_start and timeframe_end %}
                {{- dbt_utils.datediff(timeframe_start, timeframe_end, 'hour' ) }} as timeframe_duration_hours
            {%- else %}
                null as timeframe_duration_hours
            {%- endif %}
        from
            union_metrics
        where metric_name is not null
        and metric_value < {{ var('max_int') }}

    )

    select * from metrics_final )


{% endmacro %}

