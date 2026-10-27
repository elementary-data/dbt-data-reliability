{% macro get_duplicated_values(values) %}
    {#- Returns the values that appear more than once, each of them once. -#}
    {% set seen_values = {} %}
    {% set duplicated_values = [] %}
    {% for value in values %}
        {% if value not in seen_values %} {% do seen_values.update({value: false}) %}
        {% elif not seen_values[value] %}
            {% do seen_values.update({value: true}) %}
            {% do duplicated_values.append(value) %}
        {% endif %}
    {% endfor %}
    {% do return(duplicated_values) %}
{% endmacro %}
