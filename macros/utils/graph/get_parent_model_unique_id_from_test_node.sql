{% macro get_parent_model_unique_id_from_test_node(test_node) %}
    {% set test_depends_on = test_node.get('depends_on') %}
    {% if test_depends_on %}
        {% set depends_on_nodes = test_depends_on.get('nodes') %}
        {% if depends_on_nodes %}
            {% if depends_on_nodes | length == 1 %}
                {{ return(depends_on_nodes[0]) }}
            {% else %}
                {% set current_package_name = test_node.get('package_name') %}
                {% if current_package_name %}
                    {% set current_package_name = '.' ~ current_package_name ~ '.' %}
                    {% set nodes_in_current_pacakgae = [] %}
                    {% for node in depends_on_nodes %}
                        {% if current_package_name in node %}
                            {% do nodes_in_current_pacakgae.append(node) %}
                        {% endif %}
                    {% endfor %}
                    {% if nodes_in_current_pacakgae | length == 1 %}
                        {{ return(nodes_in_current_pacakgae[0]) }}
                    {% endif %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endif %}
    {{ return(none) }}
{% endmacro %}
