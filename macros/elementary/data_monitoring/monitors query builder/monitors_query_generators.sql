{% macro table_monitors_query(full_table_name, timestamp_column, days_back, timeframe_duration, table_monitors, column_config, should_backfill, timestamp_column_data_type, thread_number) %}

    {%- set thread_insert_model = 'init_data_monitors_thread_' ~ thread_number %}
    {%- set start_msg = 'Started running data monitors on table: ' ~ full_table_name %}
    {%- set end_msg = 'Finished running data monitors on table: ' ~ full_table_name %}
    {%- set pass_msg = 'No need to run datamonitors on table: ' ~ full_table_name %}
    {%- set backfill_message = 'Running full backfill on table ' ~ full_table_name %}

    {%- if timestamp_column_data_type == 'string' %}
        {%- set is_timestamp = elementary.try_cast_column_to_timestamp(full_table_name, timestamp_column) %}
    {%- elif timestamp_column_data_type == 'timestamp' %}
        {%- set is_timestamp = true %}
    {%- else %}
        {%- set is_timestamp = false %}
    {%- endif %}

    {%- set max_timeframe_end = "'" ~ elementary.max_timeframe_end(timeframe_duration) ~ "'" -%}

    {% if timestamp_column is defined and timestamp_column is not none and is_timestamp is sameas true %}
        {%- if should_backfill is sameas true -%}
            {%- set timeframes = (days_back * 24 / timeframe_duration) | int -%}
            {%- if timeframes >= 1 -%}
                {% do elementary.edr_log(start_msg) %}
                {% do edr_log(backfill_message) %}
                {% do elementary.insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type, thread_insert_model) %}
                {% do elementary.edr_log(end_msg) %}
            {%- else %}
                {% do elementary.edr_log(pass_msg) %}
            {%- endif %}
        {%- else -%}
            {%- set hours_back = elementary.hours_since_last_run(days_back, max_timeframe_end) -%}
            {%- set timeframes = (hours_back / timeframe_duration) | int -%}
            {%- if timeframes >= 1 -%}
                {% do elementary.edr_log(start_msg) %}
                {%- if hours_back == days_back*24 %}
                    {% do edr_log(backfill_message) %}
                {%- endif %}
                {% do elementary.insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type, thread_insert_model) %}
                {% do elementary.edr_log(end_msg) %}
            {%- else %}
                {% do elementary.edr_log(pass_msg) %}
            {%- endif %}
        {% endif %}
    {%- else -%}
        {%- set hours_back = elementary.hours_since_last_run(days_back, max_timeframe_end) -%}
        {%- if hours_back is not none and hours_back >= timeframe_duration %}
            {% do elementary.edr_log(start_msg) %}
            {%- set one_bucket_query = elementary.one_bucket_monitors_query(full_table_name, null, null, null, null, table_monitors, column_config) -%}
            {%- set insert_one_bucket = elementary.insert_as_select(thread_insert_model, one_bucket_query) %}
            {%- do run_query(insert_one_bucket)%}
            {% do elementary.edr_log(end_msg) %}
        {%- else %}
            {% do elementary.edr_log(pass_msg) %}
        {%- endif %}
    {%- endif -%}

{% endmacro %}


{% macro insert_metrics_to_table(timeframes, max_timeframe_end, full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type, thread_insert_model) %}
    {%- for i in range(timeframes) -%}
        {%- set time_diff_end = -(i * timeframe_duration) -%}
        {%- set time_diff_start = -((i + 1) * timeframe_duration) -%}
        {%- set timeframe_end = dbt_utils.dateadd('hour', time_diff_end , max_timeframe_end) -%}
        {%- set timeframe_start = dbt_utils.dateadd('hour', time_diff_start , max_timeframe_end) -%}

        {%- set one_bucket_query = elementary.one_bucket_monitors_query(full_table_name, timestamp_column, timeframe_start, timeframe_end, timeframe_duration, table_monitors, column_config, timestamp_column_data_type) -%}
        {%- set insert_one_bucket = elementary.insert_as_select(thread_insert_model, one_bucket_query) %}
        {%- do run_query(insert_one_bucket) %}
    {%- endfor -%}
{% endmacro %}