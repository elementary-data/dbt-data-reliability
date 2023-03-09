{% macro list_concat_with_separator(item_list, separator, handle_nulls = true) %}
    {{ return(adapter.dispatch('list_concat_with_separator','elementary')(item_list, separator, handle_nulls = true)) }}
{% endmacro %}

{% macro default__list_concat_with_separator(item_list, separator, handle_nulls = true) %}
    {% set new_list = [] %}
    {% for item in item_list %}
        {% set new_item = item %}
        {% if handle_nulls %}
            {% set new_item = "case when " ~ elementary.cast_as_string(item) ~ " is null then 'NULL' else " ~ elementary.cast_as_string(item) ~ " end" %}
        {% endif %}
        {% do new_list.append(new_item) %}
        {% if not loop.last %}
            {% do new_list.append(elementary.quote(separator)) %}
        {% endif %}
    {% endfor %}
    {{ return(elementary.join_list(new_list, " || ")) }}
{% endmacro %}

{% macro sqlserver__list_concat_with_separator(item_list, separator, handle_nulls = true) %}
    {% set new_list = [] %}
    {% for item in item_list %}
        {% set new_item = item %}
        {% if handle_nulls %}
            {% set new_item = "case when " ~ elementary.cast_as_string(elementary.quote(item)) ~ " is null then 'NULL' else " ~ elementary.cast_as_string(elementary.quote(item)) ~ " end" %}
        {% endif %}
        {% do new_list.append(new_item) %}
        {% if not loop.last %}
            {% do new_list.append(elementary.quote(separator)) %}
        {% endif %}
    {% endfor %}
    {{ return(elementary.join_list(new_list, " + ")) }}
{% endmacro %}