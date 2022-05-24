{% macro insensitive_get_dict_value(dict, key, default) -%}
    {%- set value = dict.get(key) %}
    {%- if value -%}
        {{- return(value) -}}
    {%- endif %}
    {%- set value = dict.get(key.lower()) %}
    {%- if value -%}
        {{- return(value) -}}
    {%- endif %}
    {%- set value = dict.get(key.upper()) %}
    {%- if value -%}
        {{- return(value) -}}
    {%- endif %}
    {%- if default -%}
        {{- return(default) -}}
    {% else %}
        {{ return(none) }}
    {% endif %}
{%- endmacro %}
