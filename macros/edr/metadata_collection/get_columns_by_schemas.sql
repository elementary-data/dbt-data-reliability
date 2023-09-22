{% macro get_columns_by_schemas(configured_schemas) %}
    {# if 'dbt run --select elementary' is executed, then the variable is set to true #}
    {% set is_selected_elementary = ('elementary' in context["invocation_args_dict"]["select"]) %}

    {%- if configured_schemas | length > 0 and not is_selected_elementary -%}
        {{ elementary.union_macro_queries(configured_schemas, elementary.get_columns_from_information_schema) }}
    {%- else %}
        {{ elementary.get_empty_columns_from_information_schema_table() }}
    {%- endif %}
{% endmacro %}
