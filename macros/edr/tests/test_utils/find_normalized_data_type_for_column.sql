{% macro find_normalized_data_type_for_column(model_relation, column_name) %}

{% set columns_from_relation = adapter.get_columns_in_relation(model_relation) %}
{% if columns_from_relation and columns_from_relation is iterable %}
    {% for column_obj in columns_from_relation %}
        {% if column_obj.column | lower == column_name | lower %}
            {{ return(elementary.normalize_data_type(column_obj.dtype)) }}
        {% endif %}
    {% endfor %}
{% endif %}
{{ return(none) }}

{% endmacro %}
