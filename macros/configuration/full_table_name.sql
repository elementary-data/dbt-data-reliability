{% macro full_table_name() -%}
    upper(concat(db_name, '.', schema_name, '.', table_name)) as full_table_name
{%- endmacro %}
