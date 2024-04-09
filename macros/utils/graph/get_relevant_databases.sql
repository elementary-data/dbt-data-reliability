{% macro get_relevant_databases() %}
    {% set database_names = [target.database] %}
    {% set models = graph.nodes.values() | selectattr('resource_type', '==', 'model') | list %}
    {% set sources = graph.sources.values() | selectattr('resource_type', '==', 'source') | list %}
    {% set nodes = models + sources %}
    {% for node in nodes %}
        {% set database_name = node.get('database') %}
        {% if database_name %}
            {% do database_names.append(database_name) %}
        {% endif %}
    {% endfor %}
    {% set unique_database_names = database_names | unique | list %}
    {% do return(unique_database_names) %}
{% endmacro %}
