{% macro table_monitors_query(table_to_monitor, timestamp_field, days_back, timeframe_duration, table_monitors, column_config, should_backfill) %}

    {%- set max_timeframe_end = "'" ~ max_timeframe_end(timeframe_duration) ~ "'" -%}

    {% if should_backfill is sameas true and timestamp_field %}

        {%- set timeframes = (days_back*24/timeframe_duration)|int %}
        {%- if timeframes > 0 %}
            {%- for i in range(timeframes) -%}
                {%- set time_diff_end = -(i*timeframe_duration) -%}
                {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
                {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
                {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

                {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config) -}}
                {%- if not loop.last %} union all {%- endif %}

            {%- endfor -%}
        {%- endif %}

    {% elif should_backfill is sameas false and timestamp_field %}

        {%- set hours_back = timeframe_to_query(days_back) %}
        {%- set timeframes = (hours_back/timeframe_duration)|int %}
        {%- if timeframes > 0 %}
            {%- for i in range(timeframes) -%}
                {%- set time_diff_end = -(i*timeframe_duration) -%}
                {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
                {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
                {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

                {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config) -}}
                {%- if not loop.last %} union all {%- endif %}

            {%- endfor -%}
        {%- endif %}

    {% else %}
        {%- set hours_back = timeframe_to_query(days_back) %}
        {%- set timeframes = (hours_back/timeframe_duration)|int %}
        {%- if timeframes > 0 %}
            {{- one_bucket_monitors_query(table_to_monitor, timestamp_field, null, null, null, table_monitors, column_config) -}}
            {%- if not loop.last %} union all {%- endif %}
        {%- endif %}

    {% endif %}

{% endmacro %}