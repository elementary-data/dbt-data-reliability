{% macro get_monitor_macro(monitor) %}

    {%- set macro_name = monitor + '_monitor' -%}
    {%- if context['elementary_data_reliability'].get(macro_name) -%}
        {%- set monitor_macro = context['elementary_data_reliability'][macro_name] -%}
    {%- else -%}
        {%- set monitor_macro = context['elementary_data_reliability']['no_monitor'] -%}
    {%- endif -%}

    {{- return(monitor_macro) -}}

{% endmacro %}


{% macro no_monitor() -%}
    'Monitor macro missing'
{%- endmacro %}