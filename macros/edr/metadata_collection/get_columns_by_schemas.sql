{% macro get_columns_by_schemas(configured_schemas) %}
    {%- if configured_schemas | length > 0 -%}
        {{ elementary.union_macro_queries(configured_schemas, elementary.get_columns_from_information_schema) }}
    {%- else %}
        {{ elementary.get_empty_columns_from_information_schema_table() }}
    {%- endif %}
{% endmacro %}
