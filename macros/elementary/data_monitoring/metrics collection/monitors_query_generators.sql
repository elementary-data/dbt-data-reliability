{% macro table_monitors_query(full_table_name, timestamp_column, days_back, timeframe_duration, table_monitors, column_config, should_backfill, timestamp_column_data_type) %}

    {%- if timestamp_column_data_type == 'string' %}
        {%- set is_timestamp = elementary.try_cast_column_to_timestamp(full_table_name, timestamp_column) %}
    {%- elif timestamp_column_data_type == 'datetime' %}
        {%- set is_timestamp = true %}
    {%- else %}
        {%- set is_timestamp = false %}
    {%- endif %}

    {%- set max_timeframe_end = "'" ~ elementary.max_timeframe_end(timeframe_duration) ~ "'" -%}

    {% if timestamp_column is defined and timestamp_column is not none and is_timestamp is sameas true %}
        {%- if should_backfill is sameas true -%}
            {%- set timeframes = (days_back * 24 / timeframe_duration) -%}
            {%- if timeframes >= 1 -%}
                {% do elementary.insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) %}
            {%- endif %}
        {%- else -%}
            {%- set hours_back = elementary.hours_since_last_run(days_back, max_timeframe_end) -%}
            {%- set timeframes = (hours_back / timeframe_duration) -%}
            {%- if timeframes >= 1 -%}
                {% do elementary.insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) %}
            {%- endif %}
        {% endif %}
    {%- else -%}
        {%- set one_bucket_query = elementary.one_bucket_monitors_query(full_table_name, null, null, null, null, table_monitors, column_config) -%}
        {%- set insert_one_bucket = elementary.insert_as_select('temp_monitoring_metrics', one_bucket_query) %}
        {%- do run_query(insert_one_bucket)%}
    {%- endif -%}

{% endmacro %}


{% macro insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) %}
    {%- for i in range(timeframes) -%}
        {%- set time_diff_end = -(i * timeframe_duration) -%}
        {%- set time_diff_start = -((i + 1) * timeframe_duration) -%}
        {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
        {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

        {%- set one_bucket_query = elementary.one_bucket_monitors_query(full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) -%}
        {%- set insert_one_bucket = elementary.insert_as_select('temp_monitoring_metrics', one_bucket_query) %}
        {%- do run_query(insert_one_bucket) %}
    {%- endfor -%}
{% endmacro %}