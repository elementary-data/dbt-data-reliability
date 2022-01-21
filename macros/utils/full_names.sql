{% macro full_table_name() -%}
    upper(concat(database_name, '.', schema_name, '.', table_name)) as full_table_name
{%- endmacro %}

{% macro full_table_name_to_schema() -%}
    substr(upper(full_table_name), 1, regexp_instr(full_table_name , '\\.' ,1, 2)-1) as full_schema_name
{%- endmacro %}

{% macro full_schema_name() -%}
    upper(concat(database_name, '.', schema_name)) as full_schema_name
{%- endmacro %}

{% macro full_name_to_db() -%}
    trim(split(full_table_name,'.')[0],'"') as database_name
{%- endmacro %}

{% macro full_name_to_schema() -%}
    trim(split(full_table_name,'.')[1],'"') as schema_name
{%- endmacro %}

{% macro full_name_to_table() -%}
    trim(split(full_table_name,'.')[2],'"') as table_name
{%- endmacro %}
