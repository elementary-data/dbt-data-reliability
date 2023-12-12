{% macro undefined_dict_keys_to_none(dict) %}
    {% for key in dict %}
        {% if dict[key] is not defined %}
            {% do dict.update({key: none}) %}
        {% endif %}
    {% endfor %}
    {{ return(dict) }}
{% endmacro %}