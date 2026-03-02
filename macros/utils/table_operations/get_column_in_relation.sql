{% macro get_column_in_relation(relation, column_name) %}
    {% set columns = adapter.get_columns_in_relation(relation) %}
    {% for column in columns %}
        {% if column.name == column_name %}
            {% do return(column) %}
        {% endif %}
    {% endfor %}
    {% do return(none) %}
{% endmacro %}
