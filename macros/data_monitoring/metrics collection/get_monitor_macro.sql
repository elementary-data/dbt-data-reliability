{% macro get_monitor_macro(monitor) %}

    {%- set macro_name = monitor + '_monitor' -%}
    {%- if context['elementary'].get(macro_name) -%}
        {%- set monitor_macro = context['elementary'][macro_name] -%}
    {%- else -%}
        {%- set monitor_macro = context['elementary']['no_monitor'] -%}
    {%- endif -%}

    {{- return(monitor_macro) -}}

{% endmacro %}


{% macro no_monitor(monitor) -%}
    {%- set error = 'Monitor macro for ' ~ monitor ~ ' is missing' %}
    {%- do log(error) -%}
    null
{%- endmacro %}