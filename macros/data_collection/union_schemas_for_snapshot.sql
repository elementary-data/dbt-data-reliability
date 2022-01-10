{% macro union_schemas_for_snapshot() %}

    {% set monitored_schemas = get_monitored_schemas() %}

    {% for monitored_schema in monitored_schemas %}
        {% set split_schema_name = monitored_schema.split('.') %}
        {{ get_schemas_snapshot_data(split_schema_name[0], split_schema_name[1]) }}
        {% if not loop.last %}
            union all
        {% endif %}
    {% endfor %}

{% endmacro %}