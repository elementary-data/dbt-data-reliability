{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema -%}
    {%- if not custom_schema_name -%}
        {{ default_schema }}
    {%- else -%}
        {{ custom_schema_name | replace('-', '_') | trim }}
    {%- endif -%}
{%- endmacro %}
