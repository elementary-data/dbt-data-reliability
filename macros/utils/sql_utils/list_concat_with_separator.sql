{% macro list_concat_with_separator(item_list, separator, handle_nulls = true) %}
    {% set new_list = [] %}
    {% for item in item_list %}
        {% set new_item = elementary.edr_quote(item) %}
        {% if handle_nulls %}
            {% set new_item = "case when " ~ elementary.edr_cast_as_string(item) ~ " is null then 'NULL' else " ~ elementary.edr_cast_as_string(item) ~ " end" %}
        {% endif %}
        {% do new_list.append(new_item) %}
        {% if not loop.last %}
            {% do new_list.append(elementary.edr_quote(separator)) %}
        {% endif %}
    {% endfor %}
    {{ return(elementary.join_list(new_list, " || ")) }}
{% endmacro %}