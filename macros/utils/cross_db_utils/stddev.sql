{# Standard deviation made to work on all adapters #}
{% macro edr_stddev(metric_value) -%}
    {{ return(adapter.dispatch('edr_stddev', 'elementary') (metric_value)) }}
{%- endmacro %}

{% macro default__edr_stddev(metric_value) %}
    stddev(metric_value)
{% endmacro %}

{% macro fabric__edr_stddev(metric_value) %}
   stdev(metric_value)
{% endmacro %}