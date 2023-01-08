{% macro dict_has_nonempty_values(dict) %}
    {% for val in dict.values() %}
        {% if val %}
            {% do return(true) %}
        {% endif %}
    {% endfor %}
    {% do return(false) %}
{% endmacro %}
