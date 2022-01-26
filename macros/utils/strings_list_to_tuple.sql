{% macro strings_list_to_tuple(args) %}
    {% if args|length > 0 %}
        ({% for arg in args %} '{{ arg }}' {{ "," if not loop.last else "" }} {% endfor %})
    {% else %}
        ('')
    {% endif %}
{% endmacro %}}