{% macro generate_schema_name(custom_schema_name, node) -%}
    {% set schema_name = get_default_schema_name(custom_schema_name, node) %}
    {% set schema_name_suffix_by_var = var('schema_name_suffix', '') %}
    {% if schema_name_suffix_by_var %}
        {% set schema_name = schema_name + schema_name_suffix_by_var %}
    {% endif %}

    {% do return(schema_name) %}
{%- endmacro %}

{% macro get_default_schema_name(custom_schema_name, node) -%}
    {% do return(adapter.dispatch('get_default_schema_name', 'elementary_tests')(custom_schema_name, node)) %}
{% endmacro %}

{% macro default__get_default_schema_name(custom_schema_name, node) -%}
    {%- set schema_name = target.schema -%}
    {% if custom_schema_name %}
        {% set schema_name = "{}_{}".format(schema_name, custom_schema_name) %}
    {% endif %}
    {% do return(schema_name) %}
{%- endmacro %}

{% macro dremio__get_default_schema_name(custom_schema_name, node) -%}
    {%- set default_schema = target.schema if not is_datalake_node(node) else target.root_path -%}
    {%- if not custom_schema_name -%}
        {% do return(default_schema) %}
    {%- elif default_schema == 'no_schema' -%}
        {% do return(custom_schema_name) %}
    {%- else -%}
        {% do return("{}_{}".format(default_schema, custom_schema_name)) %}
    {%- endif -%}
{%- endmacro %}
