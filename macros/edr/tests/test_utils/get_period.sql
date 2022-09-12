{% macro get_period(period_value, model_node) %}
    {%- set supported_periods = ['hour', 'day'] %}
    {%- set period = elementary.get_test_argument(argument_name='period', value=period_value, model_node=model_node) %}

    {%- if period in supported_periods %}
        {{ return(period) }}
    {%- elif elementary.get_config_var('period') in supported_periods %}
        {{- elementary.debug_log('Period not supported. Using default period var.') }}
        {{ return(elementary.get_config_var('period')) }}
    {%- else %}
        {{- elementary.debug_log('Period not supported. Using default: day.') }}
        {{ return('day') }}
    {% endif %}
{% endmacro %}
