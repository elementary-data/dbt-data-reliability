{% macro get_alert_fields(test_meta = none) %}
    {% set default_alert_fields = elementary.get_config_var('alert_fields') %}
    {% set test_alert_fields = none %}
    {% if test_meta %}
        {% set test_alert_fields = elementary.safe_get_with_default(test_meta, 'alert_fields', none) %}
    {% endif %}

    {% if test_alert_fields %}
        {{ return(test_alert_fields) }}
    {% else %}
        {{ return(default_alert_fields) }}
    {% endif %}
{% endmacro %}
