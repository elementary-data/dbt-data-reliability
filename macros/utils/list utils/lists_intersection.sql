{% macro lists_intersection(list_a, list_b) %}

    {%- set new_list = [] %}

    {% for i in list_a %}
        {% if i in list_b %}
            {{ new_list.append(i) }}
        {% endif %}
    {% endfor %}

    {{ return(new_list) }}

{% endmacro %}