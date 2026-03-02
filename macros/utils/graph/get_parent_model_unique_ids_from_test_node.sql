{% macro get_parent_model_unique_ids_from_test_node(test_node) %}
    {% set nodes_in_current_package = [] %}
    {% set test_depends_on = test_node.get('depends_on') %}
    {% if test_depends_on %}
        {% set depends_on_nodes = test_depends_on.get('nodes') %}
        {% if depends_on_nodes %}
            {% set current_package_name = test_node.get('package_name') %}
            {% if current_package_name %}
                {% set current_package_name = '.' ~ current_package_name ~ '.' %}
                {% for node in depends_on_nodes %}
                    {% if current_package_name in node %}
                        {% do nodes_in_current_package.append(node) %}
                    {% endif %}
                {% endfor %}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return(nodes_in_current_package) }}
{% endmacro %}
