{% macro get_columns_monitors_config(full_table_name) %}

    {%- set monitored_columns = run_query(elementary.get_monitored_columns(full_table_name)) %}
    {%- set columns_config = [] %}

    {%- for monitored_column in monitored_columns %}
        {%- set column_name = elementary.insensitive_get_dict_value(monitored_column, 'column_name') %}
        {%- set data_type = elementary.insensitive_get_dict_value(monitored_column, 'data_type') %}
        {%- set should_backfill = elementary.insensitive_get_dict_value(monitored_column, 'should_backfill') %}
        {%- set column_monitors = elementary.insensitive_get_dict_value(monitored_column, 'column_monitors') %}

        {%- if column_monitors is not none %}
            {%- set config_column_monitors = fromjson(column_monitors) %}
        {%- endif %}

        {%- set column_monitors = elementary.column_monitors_by_type(data_type, config_column_monitors) %}
        {%- set monitored_column_dict = {'column_name': column_name, 'column_monitors': column_monitors, 'should_backfill': should_backfill} %}

        {%- do columns_config.append(monitored_column_dict) %}

    {%- endfor %}

    {{ return(columns_config) }}

{% endmacro %}