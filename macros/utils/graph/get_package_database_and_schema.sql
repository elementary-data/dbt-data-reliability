{% macro get_package_database_and_schema(package_name='elementary') %}
    {% do return(adapter.dispatch('get_package_database_and_schema', 'elementary')(package_name)) %}
{% endmacro %}

{% macro default__get_package_database_and_schema(package_name='elementary') %}
    {% if execute %}
        {% set node_in_package = graph.nodes.values()
                                 | selectattr("resource_type", "==", "model")
                                 | selectattr("package_name", "==", package_name) | first %}
        {% if node_in_package %}
            {{ return([node_in_package.database, node_in_package.schema]) }}
        {% endif %}
    {% endif %}
    {{ return([none, none]) }}
{% endmacro %}

{% macro clickhouse__get_package_database_and_schema(package_name='elementary') %}
    {% if execute %}
        {% set node_in_package = graph.nodes.values()
                                 | selectattr("resource_type", "==", "model")
                                 | selectattr("package_name", "==", package_name) | first %}
        {% if node_in_package %}
            {{ return([node_in_package.schema, node_in_package.schema]) }}
        {% endif %}
    {% endif %}
    {{ return([none, none]) }}
{% endmacro %}

{% macro dremio__get_package_database_and_schema(package_name='elementary') %}
    {% if execute %}
        {% set node_in_package = graph.nodes.values()
                                 | selectattr("resource_type", "==", "model")
                                 | selectattr("package_name", "==", package_name) 
                                 | selectattr("config.materialized", "!=", "view") | first %}
        {% if node_in_package %}
            {{ return([node_in_package.database, node_in_package.schema]) }}
        {% endif %}
    {% endif %}
    {{ return([none, none]) }}
{% endmacro %}
