{% macro get_compiled_test_id() %}
    {% set test_node = model %}
    {% set test_hash = test_node.unique_id.split(".")[-1] %}
    {% set test_name = test_node.name %}
    {% do return("{}_{}".format(test_name, test_hash)) %}
{% endmacro %}
