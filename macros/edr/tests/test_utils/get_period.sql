{% macro get_period(period, model_node) %}
    {# TODO add support for month#}
    {% set supported_values = ['hour', 'day'] %}
    {% if period is none %}
        {% set elementary_config = elementary.get_elementary_config_from_node(model_node) %}
        {% if elementary_config and elementary_config is mapping %}
            {% set period = elementary_config.get('period') %}
        {% endif %}
    {% endif %}
    {% if period is none %}
        {{- elementary.edr_log('Period not specified. Using default: day') }}
        {{ return('day') }}
    {% else %}
        {% if period in supported_values %}
            {{ return(period) }}
        {% else %}
            {{- elementary.edr_log('Period not supported. Using default: day') }}
            {{ return('day') }}
        {% endif %}
    {% endif %}
    {{- return(none) -}}
{% endmacro %}
