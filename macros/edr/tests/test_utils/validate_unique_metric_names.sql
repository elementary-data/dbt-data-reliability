{% macro validate_unique_metric_names(metrics) %}
    {% set metric_names = [] %}
    {% for metric in metrics %}
        {% if not metric.name %}
            {% do exceptions.raise_compiler_error("The 'name' argument is required for each metric.") %}
        {% endif %}
        {% if metric.name in metric_names %}
            {% do exceptions.raise_compiler_error("The metric '{}' is already defined.".format(metric.name)) %}
        {% endif %}
        {% do metric_names.append(metric.name) %}
    {% endfor %}

    {% set test_node = elementary.get_test_model() %}
    {% set parent_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(test_node) %}

    {% for graph_node in graph.nodes.values() %}
        {% if test_node.unique_id != graph_node.unique_id and graph_node.resource_type == "test" %}
            {% set test_metadata = elementary.safe_get_with_default(graph_node, 'test_metadata', {}) %}
            {% if test_metadata.namespace == "elementary" and test_metadata.name == "collect_metrics" %}
                {% set test_parent_model_unique_ids = elementary.get_parent_model_unique_ids_from_test_node(graph_node) %}
                {% if parent_model_unique_ids == test_parent_model_unique_ids %}
                    {% for metric in test_metadata.kwargs.metrics %}
                        {% if metric.name in metric_names %}
                            {% do exceptions.raise_compiler_error("The metric '{}' is already defined.".format(metric.name)) %}
                        {% endif %}
                    {% endfor %}
                {% endif %}
            {% endif %}
        {% endif %}
    {% endfor %}
{% endmacro %}
