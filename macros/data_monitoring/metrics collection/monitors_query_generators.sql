-- TODO: add here some condition about the time since last run for the no time execution

{% macro table_monitors_query(table_to_monitor, timestamp_column, days_back, timeframe_duration, table_monitors, column_config, should_backfill, timestamp_column_data_type) %}

    {%- if timestamp_column_data_type == 'string' %}
        {%- set is_timestamp = elementary.cast_timestamp_column(table_to_monitor, timestamp_column) %}
    {%- elif timestamp_column_data_type == 'datetime' %}
        {%- set is_timestamp = true %}
    {%- else %}
        {%- set is_timestamp = false %}
    {%- endif %}

    {%- set max_timeframe_end = "'" ~ elementary.max_timeframe_end(timeframe_duration) ~ "'" -%}

    {%- if should_backfill is sameas true and timestamp_column and is_timestamp -%}

        {%- set timeframes = (days_back*24/timeframe_duration)|int %}
        {%- if timeframes > 0 %}
            {%- for i in range(timeframes) -%}
                {%- set time_diff_end = -(i*timeframe_duration) -%}
                {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
                {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
                {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

                {%- set one_bucket_query = elementary.one_bucket_monitors_query(table_to_monitor, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) -%}
                {%- set insert_one_bucket = elementary.insert_as_select('temp_monitoring_metrics', one_bucket_query) %}
                {%- do run_query(insert_one_bucket)%}
            {%- endfor -%}
        {%- endif %}

    {%- elif should_backfill is sameas false and timestamp_column and is_timestamp -%}
        {%- set hours_back = timeframe_to_query(days_back) %}
        {%- set timeframes = (hours_back/timeframe_duration)|int %}
        {%- if timeframes > 0 %}
            {%- for i in range(timeframes) -%}
                {%- set time_diff_end = -(i*timeframe_duration) -%}
                {%- set time_diff_start = -((i+1)*timeframe_duration) -%}
                {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
                {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

                {%- set one_bucket_query = elementary.one_bucket_monitors_query(table_to_monitor, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) -%}
                {%- set insert_one_bucket = elementary.insert_as_select('temp_monitoring_metrics', one_bucket_query) %}
                {%- do run_query(insert_one_bucket) %}

            {%- endfor -%}
        {%- endif %}

    {%- else -%}
        {%- set one_bucket_query = elementary.one_bucket_monitors_query(table_to_monitor, null, null, null, null, table_monitors, column_config) -%}
        {%- set insert_one_bucket = elementary.insert_as_select('temp_monitoring_metrics', one_bucket_query) %}
        {%- do run_query(insert_one_bucket)%}
    {%- endif -%}

{% endmacro %}