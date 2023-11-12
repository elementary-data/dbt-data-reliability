{% macro find_normalized_data_type_for_column(model_relation, column_name) %}

{% set columns_from_relation = adapter.get_columns_in_relation(model_relation) %}
{% if column_name and columns_from_relation and columns_from_relation is iterable %}
    {% for column_obj in columns_from_relation %}
        {% if column_obj.column | lower == column_name | trim('\'\"\`') | lower %}
            {{ return(elementary.normalize_data_type(elementary.get_column_data_type(column_obj))) }}
        {% endif %}
    {% endfor %}
    {% do exceptions.raise_compiler_error("Column `{}` was not found in `{}`.".format(column_name, model_relation.name)) %}
{% endif %}
{{ return(none) }}

{% endmacro %}
