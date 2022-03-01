{% macro get_table_config_path() %}

    {%- if var('edr_cli_run') is sameas false %}
        {%- set table_monitors_config = adapter.get_relation(database=elementary.target_database(), schema=target.schema, identifier='table_monitors_config') %}
        {{ return(table_monitors_config) }}
    {%- elif var('edr_cli_run') is sameas true %}
        {%- set table_monitors_config = elementary.find_config_table('table_monitors_config') %}
    {%- endif %}
    {{ return(table_monitors_config) }}

{% endmacro %}


{% macro get_column_config_path() %}

    {%- if var('edr_cli_run') is sameas false %}
        {%- set column_monitors_config = adapter.get_relation(database=elementary.target_database(), schema=target.schema, identifier='column_monitors_config') %}
        {{ return(column_monitors_config) }}
    {%- elif var('edr_cli_run') is sameas true %}
        {%- set column_monitors_config = elementary.find_config_table('column_monitors_config') %}
    {%- endif %}
    {{ return(column_monitors_config) }}

{% endmacro %}



{% macro find_config_table(config_table_name) %}
    {%- set database = elementary.target_database() %}
    {%- set info_schema_tables = elementary.from_information_schema('tables', database) %}
    {%- set query %}
        select
            concat(table_catalog, '.' , table_schema , '.' , table_name) as full_table_name
        from {{ info_schema_tables }}
        where lower(table_name) = '{{ config_table_name }}'
    {%- endset %}

    {%- set config_table = elementary.result_value(query) %}
    {%- if config_table %}
        {{ return(config_table) }}
    {% else %}
        {{ elementary_log('Could not find configuration table.') }}
    {%- endif %}
{% endmacro %}
