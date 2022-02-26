{% macro get_columns_monitors_config(full_table_name) %}

    {%- set monitored_columns = run_query(monitored_columns(full_table_name)) %}
    {%- if execute %}
        {%- set column_config_column_names = monitored_columns.column_names %}
    {%- endif %}

    {%- set columns_config = [] %}

    {%- for monitored_column in monitored_columns %}
        {%- set column_name = monitored_column[column_config_column_names[5]] %}
        {%- set data_type = monitored_column[column_config_column_names[6]] %}
        {%- set should_backfill = monitored_column[column_config_column_names[8]] %}

        {%- if monitored_column[column_config_column_names[7]] is not none %}
            {%- set config_column_monitors = fromjson(monitored_column[column_config_column_names[7]]) %}
        {%- endif %}

        {%- set column_monitors = column_monitors_by_type(data_type, config_column_monitors) %}
        {%- set monitored_column_dict = {'column_name': column_name, 'column_monitors': column_monitors, 'should_backfill': should_backfill} %}

        {%- do columns_config.append(monitored_column_dict) %}

    {%- endfor %}

    {{ return(columns_config) }}

{% endmacro %}