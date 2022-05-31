{% macro get_anomaly_sensitivity(sensitivity) %}
    {% if sensitivity %}
        {{ return(sensitivity) }}
    {% else %}
        {{ return(elementary.get_config_var('anomaly_sensitivity')) }}
    {% endif %}
{% endmacro %}
