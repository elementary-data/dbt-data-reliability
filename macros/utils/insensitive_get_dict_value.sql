{% macro insensitive_get_dict_value(dict, key, default) -%}
    {%- if key in dict -%}
        {{- return(dict[key]) -}}
    {%- elif key.lower() in dict -%}
        {{- return(dict[key.lower()]) -}}
    {%- elif default is defined -%}
        {{- return(default) -}}
    {% else %}
        {{ return(none) }}
    {% endif %}
{%- endmacro %}