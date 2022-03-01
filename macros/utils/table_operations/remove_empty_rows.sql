{% macro remove_empty_rows(table_name) %}

    {%- set columns = adapter.get_columns_in_relation(table_name) -%}
    {%- set delete_empty_rows_query %}
    delete from {{ table_name }} where {% for column in columns -%} {{ column.name }} is NULL {{- " and " if not loop.last else "" -}} {%- endfor -%}
    {%- endset %}
    {%- do run_query(delete_empty_rows_query) %}

{% endmacro %}
