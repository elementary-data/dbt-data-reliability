{% macro insensitive_get_dict_value(dict, key, default) -%}
    {% set value = elementary.safe_get_with_default(dict, key) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {% set value = elementary.safe_get_with_default(dict, key.lower()) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {% set value = elementary.safe_get_with_default(dict, key.upper()) %}
    {%- if value is not none -%}
        {{- return(value) -}}
    {%- endif %}
    {%- if default is defined -%}
        {{- return(default) -}}
    {% else %}
        {{ return(none) }}
    {% endif %}
{%- endmacro %}
