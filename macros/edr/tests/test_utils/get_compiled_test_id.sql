{% macro get_compiled_test_id() %}
    {% set test_hash = model.unique_id.split(".")[-1] %}
    {% set test_name = model.name %}
    {% do return("{}_{}".format(test_name, test_hash)) %}
{% endmacro %}
