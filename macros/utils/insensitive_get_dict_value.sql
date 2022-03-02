{% macro insensitive_get_dict_value(dict, key, default) -%}
    {%- set value = dict.get(key) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {%- set value = dict.get(key.lower()) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {%- set value = dict.get(key.upper()) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {%- if default is defined -%}
        {{- return(default) -}}
    {% else %}
        {{ return(none) }}
    {% endif %}
{%- endmacro %}