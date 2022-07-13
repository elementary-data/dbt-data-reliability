{% macro get_models_schemas_from_graph() %}
    {# Returns a list of tuples (db,schema) of the schemas of all the models in the project #}
    {% set models_schemas = [] %}
    {% if execute %}
        {% for model_node in graph.nodes.values() | selectattr('resource_type', '==', 'model') %}
            {% if adapter.check_schema_exists(model_node['database'], model_node['schema']) %}
                {% do models_schemas.append((model_node['database'], model_node['schema'])) %}
            {% endif %}
        {% endfor %}
    {% endif %}
    {{ return(models_schemas | unique | list ) }}
{% endmacro %}