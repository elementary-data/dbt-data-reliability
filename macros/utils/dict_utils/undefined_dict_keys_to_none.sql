{% macro undefined_dict_keys_to_none(dict) %}
    {% for key in dict %}
        {% if dict[key] is not defined %}
            {% set dict = elementary.dict_merge(dict, {key: none}) %}
        {% endif %}
    {% endfor %}
    {{ return(dict) }}
{% endmacro %}