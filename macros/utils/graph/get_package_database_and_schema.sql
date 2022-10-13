{% macro get_package_database_and_schema(package_name='elementary') %}
    {% if execute %}
        {% set node_in_package = graph.nodes['model.elementary.dbt_run_results'] %}
        {{ return([node_in_package.database, node_in_package.schema]) }}
    {% endif %}
    {{ return([none, none]) }}
{% endmacro %}
