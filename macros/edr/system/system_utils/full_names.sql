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


{% macro clickhouse__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        {%- set part_index = 1 -%}
    {%- elif part_name == 'schema_name' -%}
        {%- set part_index = 2 -%}
    {%- elif part_name == 'table_name' -%}
        {%- set part_index = 3 -%}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
    trim(BOTH '"' FROM splitByChar('.', full_table_name)[{{ part_index }}]) AS {{ part_name }}
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


{% macro postgres__full_name_split(part_name) %}
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

{% macro athena__full_name_split(part_name) %}
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


{% macro databricks__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        {%- set part_index = 0 -%}
    {%- elif part_name == 'schema_name' -%}
        {%- set part_index = 1 -%}
    {%- elif part_name == 'table_name' -%}
        {%- set part_index = 2 -%}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
    trim('"' from split(full_table_name,'[.]')[{{ part_index }}]) as {{ part_name }}
{% endmacro %}


{% macro dremio__full_name_split(part_name) %}
    {%- if part_name == 'database_name' -%}
        trim('"' from split_part(full_table_name,'.',1)) as {{ part_name }}
    {%- elif part_name == 'schema_name' -%}
        trim('"' from substr(full_table_name, length(split_part(full_table_name,'.',1)) + 2, 
             length(full_table_name) - length(split_part(full_table_name,'.',1)) - length(split_part(full_table_name,'.',length(full_table_name) - length(replace(full_table_name,'.','')) + 1)) - 2)) as {{ part_name }}
    {%- elif part_name == 'table_name' -%}
        trim('"' from split_part(full_table_name,'.',length(full_table_name) - length(replace(full_table_name,'.','')) + 1)) as {{ part_name }}
    {%- else -%}
        {{ return('') }}
    {%- endif -%}
{% endmacro %}


{% macro relation_to_full_name(relation) %}
    {%- if relation.is_cte %}
        {# Ephemeral models don't have db and schema #}
        {%- set full_table_name = relation.identifier | upper %}
    {%- elif relation.database %}
        {%- set full_table_name = relation.database | upper ~'.'~ relation.schema | upper ~'.'~ relation.identifier | upper %}
    {%- else %}
        {# Databricks doesn't always have a database #}
        {%- set full_table_name = relation.schema | upper ~'.'~ relation.identifier | upper %}
    {%- endif %}
    {{ return(full_table_name) }}
{% endmacro %}


{% macro model_node_to_full_name(model_node) %}
    {% set identifier = model_node.identifier or model_node.alias %}
    {%- if model_node.database %}
        {%- set full_table_name = model_node.database | upper ~'.'~ model_node.schema | upper ~'.'~ identifier | upper %}
    {%- else %}
    {# Databricks doesn't always have a database #}
        {%- set full_table_name = model_node.schema | upper ~'.'~ identifier | upper %}
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