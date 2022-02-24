{% macro from_information_schema(info_schema_view, schema_name, database_name) %}
    {{ adapter.dispatch('from_information_schema')(info_schema_view, schema_name, database_name) }}
{% endmacro %}

{% macro default__from_information_schema(info_schema_view, schema_name, database_name) %}
    {%- if database_name -%} {{ database_name}}.{%- endif -%}INFORMATION_SCHEMA.{{ info_schema_view }}
{% endmacro %}

{% macro bigquery__from_information_schema(info_schema_view, schema_name, database_name) %}
    {%- if database_name -%} `{{ database_name }}`.{%- endif -%}`{{ schema_name }}`.`INFORMATION_SCHEMA`.`{{ info_schema_view }}`
{% endmacro %}