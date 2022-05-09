{% macro assert_value(value, expected_value) %}
    {% if value != expected_value %}
        {% do elementary.edr_log("FAILED: value " ~ value ~ " does not equal to " ~ expected_value) %}
        {{ return(1) }}
    {% else %}
        {% do elementary.edr_log("SUCCESS") %}
        {{ return(0) }}
    {% endif %}
{% endmacro %}