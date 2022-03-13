{% macro get_table_monitoring_config(full_table_name) %}
    -- depends_on: {{ ref('final_columns_config') }}
    -- depends_on: {{ ref('final_tables_config') }}
    -- depends_on: {{ ref('final_should_backfill') }}
    {%- set config_query = elementary.get_monitored_table_config_query(full_table_name) %}
    {%- set table_config = elementary.result_row_to_dict(config_query) %}

    {%- if execute%}
        {%- set full_table_name = elementary.insensitive_get_dict_value(table_config, 'full_table_name') %}
        {%- set timestamp_column = elementary.insensitive_get_dict_value(table_config, 'timestamp_column') %}
        {%- set columns_monitored = elementary.insensitive_get_dict_value(table_config, 'columns_monitored') %}
        {%- set timestamp_column_data_type = elementary.insensitive_get_dict_value(table_config, 'timestamp_column_data_type') %}
        {%- set table_monitors_str = elementary.insensitive_get_dict_value(table_config, 'table_monitors') %}

        {%- set final_table_monitors = none %}
        {%- set default_table_monitors = elementary.get_default_table_monitors() %}
        {%- if table_monitors_str is not none %}
            {%- set configured_table_monitors = fromjson(table_monitors_str) %}
        {%- endif %}
        {%- if configured_table_monitors is defined and configured_table_monitors is not none and configured_table_monitors | length > 0 %}
            {%- set final_table_monitors = configured_table_monitors %}
        {%- else %}
            {%- set final_table_monitors = default_table_monitors %}
        {%- endif %}
        {# schema_changes is a different flow #}
        {% if 'schema_changes' in final_table_monitors %}
            {%- do final_table_monitors.remove('schema_changes') %}
        {% endif %}
        {% do table_config.update({'final_table_monitors': final_table_monitors}) %}

        {%- if timestamp_column_data_type == 'string' %}
            {%- set is_timestamp = elementary.try_cast_column_to_timestamp(full_table_name, timestamp_column) %}
        {%- elif timestamp_column_data_type == 'timestamp' %}
            {%- set is_timestamp = true %}
        {%- else %}
            {%- set is_timestamp = false %}
        {%- endif %}
        {% do table_config.update({'is_timestamp': is_timestamp}) %}

        {% set column_monitors = none %}
        {%- if columns_monitored is sameas true %}
            {%- set final_column_monitors = elementary.get_columns_monitors_config(full_table_name) %}
        {%- endif %}
        {% do table_config.update({'columns_config': final_column_monitors}) %}

        {%- set should_backfill_query %}
            select min_timeframe_start
            from {{ ref('final_should_backfill') }}
                where full_table_name = '{{ full_table_name }}'
        {%- endset %}
        {%- set timeframe_start = elementary.result_value(should_backfill_query) %}
        {% do table_config.update({'timeframe_start': timeframe_start}) %}
    {%- endif %}

    {{ return(table_config) }}
{% endmacro %}