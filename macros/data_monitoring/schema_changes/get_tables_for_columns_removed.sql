{% macro get_tables_for_columns_removed() %}
    {%- set tables_query %}
        select upper(full_table_name) as full_table_name
        from {{ elementary.get_table_config_path() }}
        where full_table_name is not null
    {%- endset %}
    {%- set tables_list = result_column_to_list(tables_query) %}
    {%- set tables = strings_list_to_tuple(tables_list) %}
    {{ return(tables) }}
{% endmacro %}