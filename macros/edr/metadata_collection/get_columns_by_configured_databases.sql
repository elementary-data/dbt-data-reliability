{% macro get_columns_by_configured_databases(configured_databases) %}
    {%- if configured_databases | length > 0 -%}
        {{ elementary.union_macro_queries(configured_databases, elementary.get_columns_from_information_schema) }}
    {%- else %}
        {{ elementary.empty_table([('full_table_name', 'string'), ('database_name', 'string'), ('schema_name', 'string'), ('table_name', 'string'), ('column_name', 'string'), ('data_type', 'string')]) }}
    {%- endif %}
{% endmacro %}
