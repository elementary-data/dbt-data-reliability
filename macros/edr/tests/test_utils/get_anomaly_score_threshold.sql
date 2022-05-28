{% macro get_anomaly_score_threshold(anomaly_threshold) %}
    {% if anomaly_threshold %}
        {{ return(anomaly_threshold) }}
    {% else %}
        {{ return(elementary.get_config_var('anomaly_score_threshold')) }}
    {% endif %}
{% endmacro %}
