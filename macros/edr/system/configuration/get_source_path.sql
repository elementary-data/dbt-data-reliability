{% macro get_source_path(source_name, source_table_name) %}

    {%- if var('edr_cli_run') is sameas false %}
        {%- set table_monitors_config = source(source_name, source_table_name) %}
        {{ return(table_monitors_config) }}
    {%- elif var('edr_cli_run') is sameas true %}
        {%- set table_monitors_config = elementary.find_source_table(source_table_name) %}
    {%- endif %}
    {{ return(table_monitors_config) }}

{% endmacro %}


{% macro find_source_table(source_table_name) %}
    {%- set database = adapter.database %}
    {%- set info_schema_tables = elementary.from_information_schema('tables', database) %}
    {%- set query %}
        select
            concat(table_catalog, '.' , table_schema , '.' , table_name) as full_table_name
        from {{ info_schema_tables }}
        where lower(table_name) = '{{ source_table_name }}'
    {%- endset %}

    {%- set source_table = elementary.result_value(query) %}
    {%- if source_table %}
        {{ return(source_table) }}
    {% else %}
        {%- set no_table_msg = 'Did not find table: ' ~ source_table_name %}
        {{ elementary.edr_log(no_table_msg) }}
    {%- endif %}
{% endmacro %}
