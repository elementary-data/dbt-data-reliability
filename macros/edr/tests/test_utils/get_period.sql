{% macro get_period(period, model_node) %}
    {# TODO add support for month#}
    {% set supported_values = ['hour', 'day'] %}
    {% if period is none %}
        {% set elementary_config = elementary.get_elementary_config_from_node(model_node) %}
        {% if elementary_config and elementary_config is mapping %}
            {% set period = elementary_config.get('period').lower() %}
        {% endif %}
    {% endif %}
    {% if period in supported_values %}
        {{ return(period) }}
    {% else %}
        {{- elementary.edr_log('Specified period not supported. Using default: day') }}
        {{ return('day') }}
    {% endif %}
    {{- return(none) -}}
{% endmacro %}
