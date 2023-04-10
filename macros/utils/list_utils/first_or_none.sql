{% macro first_of_list_not_none() %}
    {% for argument in varargs %}
        {# cant do if argument because I want to support passing false in here. #}
        {% if argument != none %}
            {% do return(argument) %}
        {% endif %}
    {% endfor %}
    {% do return(none)%}
{% endmacro %}
