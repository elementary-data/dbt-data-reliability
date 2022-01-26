{% macro query_different_schemas(query_macro, full_schema_names_list) %}

    {% for schema_name in full_schema_names_list %}
        {% set split_schema_name = full_schema_names_list[loop.index0].split('.') %}
        {{ query_macro(split_schema_name[0], split_schema_name[1]) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

{% endmacro %}