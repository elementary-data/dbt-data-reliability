{% macro full_table_name(alias) -%}
    {% if alias is defined %}{%- set alias_dot = alias ~ '.' %}{% endif %}
    upper({{ alias_dot }}database_name || '.' || {{ alias_dot }}schema_name || '.' || {{ alias_dot }}table_name)
{%- endmacro %}


{% macro full_schema_name() -%}
    upper(database_name || '.' || schema_name)
{%- endmacro %}


{% macro full_column_name() -%}
    upper(database_name || '.' || schema_name || '.' || table_name || '.' || column_name)
{%- endmacro %}


{% macro split_full_table_name_to_vars(full_table_name) %}
    {% set split_full_table_name = full_table_name.split('.') %}
    {# Databricks full name sometimes is schema.table, no db #}
    {%- if split_full_table_name | length == 2 %}
        {% set database_name = None %}
        {% set schema_name = split_full_table_name[0] %}
        {% set table_name = split_full_table_name[1] %}
    {%- else  %}
        {% set database_name = split_full_table_name[0] %}
        {% set schema_name = split_full_table_name[1] %}
        {% set table_name = split_full_table_name[2] %}
    {%- endif %}
    {{ return((database_name, schema_name, table_name)) }}
{% endmacro %}




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


{% macro redshift__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        {%- set part_index = 1 -%}
    {%- elif part_name == 'schema_name' -%}
        {%- set part_index = 2 -%}
    {%- elif part_name == 'table_name' -%}
        {%- set part_index = 3 -%}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
    trim(split_part(full_table_name,'.',{{ part_index }}),'"') as {{ part_name }}
{% endmacro %}


{% macro relation_to_full_name(relation) %}
    {%- if relation.database %}
        {%- set full_table_name = relation.database | upper ~'.'~ relation.schema | upper ~'.'~ relation.identifier | upper %}
    {%- else %}
    {# Databricks doesn't always have a database #}
        {%- set full_table_name = relation.schema | upper ~'.'~ relation.identifier | upper %}
    {%- endif %}
    {{ return(full_table_name) }}
{% endmacro %}


{% macro configured_schemas_from_graph_as_tuple() %}

    {%- set configured_schema_tuples = elementary.get_configured_schemas_from_graph() %}
    {%- set schemas_list = [] %}

    {%- for configured_schema_tuple in configured_schema_tuples %}
        {%- set database_name, schema_name = configured_schema_tuple %}
        {%- set full_schema_name = database_name | upper ~ '.' ~ schema_name | upper %}
        {%- do schemas_list.append(full_schema_name) -%}
    {%- endfor %}

    {% set schemas_tuple = elementary.strings_list_to_tuple(schemas_list) %}
    {{ return(schemas_tuple) }}

{% endmacro %}