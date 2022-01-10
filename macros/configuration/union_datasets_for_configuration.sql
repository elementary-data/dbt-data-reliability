{% macro union_columns_from_monitored_schemas() %}

    {% set monitored_schemas = get_monitored_schemas() %}

    {% for monitored_schema in monitored_schemas %}
        {% set split_schema_name = monitored_schema.split('.') %}
        {{ get_columns_from_monitored_schema(split_schema_name[0], split_schema_name[1]) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

{% endmacro %}