{% macro where_in_list(field, strings, exclude=False) %}
    {%- if strings|length > 0 -%}
        {%- if exclude -%}
            lower({{ field }}) not in {{ lower_strings_list_to_tuple(strings) }}
        {%- else -%}
            lower({{ field }}) in {{ lower_strings_list_to_tuple(strings) }}
        {%- endif -%}
    {%- else -%}
        1=1
    {%- endif -%}
{% endmacro %}


{% macro like_any_string_from_list(field, strings, left_string='', right_string='') %}
    {%- if strings|length > 0 -%}
        lower({{ field }}) like any {{ like_strings_list_to_tuple(strings, left_string, right_string) }}
    {%- else -%}
        1=1
    {%- endif -%}
{% endmacro %}


