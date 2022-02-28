{% macro from_information_schema(info_schema_view, database_name, schema_name) %}
    {{ adapter.dispatch('from_information_schema','elementary')(info_schema_view, database_name, schema_name) }}
{% endmacro %}

{% macro default__from_information_schema(info_schema_view, database_name, schema_name) %}
    {%- if database_name -%} {{ database_name | upper }}.{%- endif -%}INFORMATION_SCHEMA.{{ info_schema_view | upper }}
{% endmacro %}

{% macro bigquery__from_information_schema(info_schema_view, database_name, schema_name) %}
    {%- if database_name -%} `{{ database_name }}`.{%- endif -%}`{{ schema_name }}`.`INFORMATION_SCHEMA`.`{{ info_schema_view }}`
{% endmacro %}