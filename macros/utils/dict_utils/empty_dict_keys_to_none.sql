{% macro empty_dict_keys_to_none(dict) %}
    {% for key in dict %}
        {% if not dict[key] %}
            {% do dict.update({key: none}) %}
        {% endif %}
    {% endfor %}
    {{ return(dict) }}
{% endmacro %}