{% macro get_table_config_path() %}

    {%- if var('edr_run') is sameas false %}
        {%- set table_monitors_config_path = adapter.get_relation(database=target_database(), schema=target.schema, identifier='table_monitors_config') %}
        {{ return(table_monitors_config_path) }}
    {%- elif var('edr_run') is sameas true %}
        {%- set table_monitors_config_path = find_config_in_target('table') %}
    {%- endif %}
    {{ return(table_monitors_config_path) }}

{% endmacro %}


{% macro get_column_config_path() %}

    {%- if var('edr_run') is sameas false %}
        {%- set column_monitors_config_path = adapter.get_relation(database=target_database(), schema=target.schema, identifier='column_monitors_config') %}
        {{ return(column_monitors_config_path) }}
    {%- elif var('edr_run') is sameas true %}
        {%- set column_monitors_config_path = find_config_in_target('column') %}
    {%- endif %}
    {{ return(column_monitors_config_path) }}

{% endmacro %}



{% macro find_config_in_target(table_or_column) %}
    {%- if 'table' in table_or_column or 'column' in table_or_column %}
        {%- set database = elementary.target_database() %}
        {%- set config_table = table_or_column ~ '_monitors_config' %}
        {%- set info_schema_tables = elementary.from_information_schema('tables', database) %}
        {%- set query %}
            select
                concat(table_catalog, '.' , table_schema , '.' , table_name) as full_table_name
            from {{ info_schema_tables }}
            where lower(table_name) = '{{ config_table }}'
        {%- endset %}

        {%- set table_config_path = elementary.result_value(query) %}
        {%- if table_config_path %}
            {{ return(table_config_path) }}
        {%- endif %}
    {%- else %}
        {{ elementary_log('Could not find configuration table.') }}
    {%- endif %}

{% endmacro %}
