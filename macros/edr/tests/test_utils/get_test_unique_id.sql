{% macro get_test_unique_id() %}
    {% set test_unique_id = model.get('unique_id') %}
    {{ return(test_unique_id) }}
{% endmacro %}
