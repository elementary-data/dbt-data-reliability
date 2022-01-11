{% macro full_table_name() -%}
    upper(concat(db_name, '.', schema_name, '.', table_name)) as full_table_name
{%- endmacro %}

{% macro full_table_name_to_schema() -%}
    substr(full_table_name, 1, regexp_instr(full_table_name, '\\.' ,1, 2)-1)
{%- endmacro %}

