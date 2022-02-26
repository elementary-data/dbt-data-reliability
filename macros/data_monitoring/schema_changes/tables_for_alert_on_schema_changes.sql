{% macro tables_for_alert_on_schema_changes() %}

    {%- set alert_on_schema_changes_list = [] %}

    {%- set monitored_tables = run_query(get_schema_changes_config_query()) %}
    {%- if execute %}
        {%- set table_config_column_names = monitored_tables.column_names %}
    {%- endif %}

    {%- for monitored_table in monitored_tables %}
        {%- set full_table_name = monitored_table[table_config_column_names[0]] %}
        {%- if monitored_table[table_config_column_names[7]] is none %}
            {%- do alert_on_schema_changes_list.append(full_table_name | upper) %}
        {%- elif monitored_table[table_config_column_names[7]] is not none %}
            {%- if 'schema_changes' in fromjson(monitored_table[table_config_column_names[7]]) %}
                {%- do alert_on_schema_changes_list.append(full_table_name | upper) %}
            {%- endif %}
        {%- endif %}
    {%- endfor %}

    {%- set alert_on_schema_changes = strings_list_to_tuple(alert_on_schema_changes_list) %}

    {{ return(alert_on_schema_changes) }}

{%- endmacro %}