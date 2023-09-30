{% macro to_sql_list(ls) %}
    {% set rendered_items = [] %}
    {% for item in ls %}
        {% do rendered_items.append(elementary.render_value(item)) %}
    {% endfor %}

({{ rendered_items | join(', ') }})
{% endmacro %}
