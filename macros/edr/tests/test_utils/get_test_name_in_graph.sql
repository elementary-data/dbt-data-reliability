{% macro get_test_name_in_graph() %}
    {% set test_name_in_graph = model.name %}
    {{ return(test_name_in_graph) }}
{% endmacro %}
