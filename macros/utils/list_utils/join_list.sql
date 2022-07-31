{% macro join_list(item_list, separator) %}
    {{ return(item_list | join(separator)) }}
{% endmacro %}

{% macro list_concat_with_separator(item_list, separator) %}
    {% set new_list = [] %}
    {% for item in item_list %}
        {% do new_list.append(item) %}
        {% if not loop.last %}
            {% do new_list.append("'" ~ separator ~ "'") %}
        {% endif %}
    {% endfor %}
    {{ return(elementary.join_list(new_list, " || ")) }}
{% endmacro %}