{% macro insensitive_get_dict_value(dict, key, default) -%}
    {%- if key in dict -%}
        {{- return(dict[key]) -}}
    {%- elif key.lower() in dict -%}
        {{- return(dict[key.lower()]) -}}
    {%- else -%}
        {{- return(default) -}}
    {% endif %}
{%- endmacro %}