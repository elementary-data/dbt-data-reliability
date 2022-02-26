{% macro strings_list_to_tuple(args) %}
    {%- if args is defined %}
        {%- if not args is none %}
            ({% for arg in args %} '{{ arg }}' {{ "," if not loop.last else "" }} {% endfor %})
        {%- endif %}
    {%- else %}
        ('')
    {%- endif %}
{% endmacro %}}