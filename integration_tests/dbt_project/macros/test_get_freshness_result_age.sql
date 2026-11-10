{% macro test_get_freshness_result_age(result_dict) %}
    {% set result = elementary.get_freshness_result_age(result_dict) %}
    {{ return(result) }}
{% endmacro %}
