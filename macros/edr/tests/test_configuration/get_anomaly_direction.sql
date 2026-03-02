{% macro get_anomaly_direction(anomaly_direction, model_graph_node) %}
    {%- set anomaly_direction = elementary.get_test_argument('anomaly_direction', anomaly_direction, model_graph_node) | lower %}
    {%- do elementary.validate_anomaly_direction(anomaly_direction) -%}
    {{ return(anomaly_direction) }}
{% endmacro %}

{% macro validate_anomaly_direction(anomaly_direction) %}
    {% if anomaly_direction %}
      {% set direction_case_insensitive = anomaly_direction %}
      {% if direction_case_insensitive not in ['drop','spike','both'] %}
        {% do exceptions.raise_compiler_error('Supported anomaly directions are: both, drop, spike. received anomaly_direction: {}'.format(anomaly_direction)) %}
      {% endif %}
    {% else %}
      {% do exceptions.raise_compiler_error('anomaly_direction can\'t be empty. Supported anomaly directions are: both, drop, spike') %}
    {% endif %}
{% endmacro %}