{% macro merge_dict2_to_dict1(dict1, dict2) %}
    {%- if dict1 and dict2 and dict1 is mapping and dict2 is mapping %}
        {% for key in dict1 %}
            {% if not dict1[key] and dict2[key] %}
                {% do dict1.update({key: dict2[key]}) %}
            {% endif %}
        {% endfor %}
    {%- endif %}
    {{ return(dict1) }}
{% endmacro %}