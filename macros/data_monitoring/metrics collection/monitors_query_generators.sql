{% macro table_monitors_cte(table_monitors) %}

    {%- if table_monitors is defined %}
        {%- for table_monitor in table_monitors -%}
            {%- set monitor_macro = get_monitor_macro(table_monitor) %}
            select
                null as column_name,
                '{{ table_monitor }}' as metric_name,
                {{ monitor_macro() }} as metric_value
            from
                timeframe_data
            {% if not loop.last %} union all {%- endif %}
        {%- endfor -%}

    {%- else %}
        {{ empty_table([('column_name', 'string'), ('column_monitor', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}


{% macro column_monitors_cte(column_config) %}

    {%- if column_config is defined and column_config != [] %}
        {%- for monitored_column in column_config -%}
            {%- set monitored_column = column_config[loop.index0]['column_name'] %}
            {%- for column_monitor in column_config[loop.index0]['column_monitors'] %}
                {%- set monitor_macro = get_monitor_macro(column_monitor) %}
                    select
                        '{{ monitored_column }}' as column_name,
                        '{{ column_monitor }}' as metric_name,
                        {{ monitor_macro(monitored_column) }} as metric_value
                    from
                        timeframe_data
                    {% if not loop.last %} union all {%- endif %}
            {%- endfor %}
            {% if not loop.last %} union all {%- endif %}
        {%- endfor -%}

    {%- else %}
        {{ empty_table([('column_name', 'string'), ('column_monitor', 'string'), ('metric_value', 'int')]) }}
    {%- endif %}

{% endmacro %}


{% macro one_bucket_monitors_query(monitored_table, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config) %}

    (
    with timeframe_data as (

        select *
        from {{ monitored_table }}
        where
        {% if timestamp_field is not none %}
            {{ timestamp_field }} > {{ timeframe_start }} and {{ timestamp_field }} < {{ timeframe_end }}
        {% else %}
            true
        {% endif %}

    ),

    table_monitors as (

        {{ table_monitors_cte(table_monitors) }}

    ),

    column_monitors as (

        {{ column_monitors_cte(column_config) }}

    ),

    union_metrics as (

        select * from table_monitors
        union all
        select * from column_monitors

    ),

    metrics_final as (

        select
            '{{ monitored_table }}' as table_name,
            column_name,
            metric_name,
            metric_value,
            {%- if timeframe_start is defined %}
                {{ timeframe_start }} as timeframe_start,
            {%- else %}
                null as timeframe_start,
            {%- endif %}
            {%- if timeframe_end is defined %}
                {{ timeframe_end }} as timeframe_end,
            {%- else %}
                null as timeframe_end,
            {%- endif %}
            {%- if timeframe_duration is defined %}
                {{ timeframe_duration }} as timeframe_duration,
            {%- else %}
                null as timeframe_end,
            {%- endif %}
            {{ run_start_column() }} as run_started_at
        from
            union_metrics
        where metric_name is not null

    )

    select * from metrics_final )


{% endmacro %}



{% macro table_monitors_query(table_to_monitor, timestamp_field, days_back, timeframe_duration, table_monitors, column_config, should_backfill) %}

    {%- set max_timeframe_end = "'" ~ max_timeframe_end(timeframe_duration) ~ "'" -%}

    {% if should_backfill and timestamp_field is not none %}

        {%- set timeframes = (days_back*24/timeframe_duration)|int %}
        {%- for i in range(timeframes) -%}
            {%- set time_diff_end = -(i*timeframe_duration) -%}
            {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
            {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
            {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

            {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config) -}}
            {%- if not loop.last %} union all {%- endif %}

        {%- endfor -%}

    {% elif should_backfill is sameas false and timestamp_field is not none %}

        {%- set hours_back = timeframe_to_query(days_back) %}
        {%- set timeframes = (hours_back/timeframe_duration)|int %}
        {%- for i in range(timeframes) -%}
            {%- set time_diff_end = -(i*timeframe_duration) -%}
            {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
            {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
            {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

            {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config) -}}
            {%- if not loop.last %} union all {%- endif %}

        {%- endfor -%}

    {% else %}

        {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, null, null, null, table_monitors, column_config) -}}

    {% endif %}

{% endmacro %}