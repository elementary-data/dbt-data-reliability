{% macro full_table_name(alias) -%}
    {% if alias is defined %}{%- set alias_dot = alias ~ '.' %}{% endif %}
    upper(concat({{ alias_dot }}database_name, '.', {{ alias_dot }}schema_name, '.', {{ alias_dot }}table_name))
{%- endmacro %}


{% macro full_schema_name() -%}
    upper(concat(database_name, '.', schema_name))
{%- endmacro %}


{% macro full_column_name() -%}
    upper(concat(database_name, '.', schema_name, '.', table_name, '.', column_name))
{%- endmacro %}


{% macro full_name_split(part_name) %}
    {{ adapter.dispatch('full_name_split','elementary')(part_name) }}
{% endmacro %}


{% macro default__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        {%- set part_index = 0 -%}
    {%- elif part_name == 'schema_name' -%}
        {%- set part_index = 1 -%}
    {%- elif part_name == 'table_name' -%}
        {%- set part_index = 2 -%}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
    trim(split(full_table_name,'.')[{{ part_index }}],'"') as {{ part_name }}
{% endmacro %}


{% macro bigquery__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        {%- set part_index = 0 %}
    {%- elif part_name == 'schema_name' -%}
        {%- set part_index = 1 %}
    {%- elif part_name == 'table_name' -%}
        {%- set part_index = 2 -%}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
    trim(split(full_table_name,'.')[OFFSET({{ part_index }})],'"') as {{ part_name }}
{% endmacro %}


{% macro relation_to_full_name(relation) %}
    {%- set full_table_name = relation.database ~'.'~ relation.schema ~'.'~ relation.identifier | upper %}
    {{ return(full_table_name) }}
{% endmacro %}