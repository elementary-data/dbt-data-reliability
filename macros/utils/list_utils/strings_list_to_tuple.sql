{% macro strings_list_to_tuple(strings) %}
    {%- if strings %}
        ({% for string in strings %} '{{ string }}' {{ "," if not loop.last else "" }} {% endfor %})
    {%- else %}
        ('')
    {%- endif %}
{% endmacro %}