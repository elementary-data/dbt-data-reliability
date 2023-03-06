{% macro get_column_obj_and_monitors(model_relation, column_name, column_tests=none) %}

    {% set column_obj_and_monitors = [] %}
    {% set column_objects = adapter.get_columns_in_relation(model_relation) %}
    {% for column_obj in column_objects %}
        {% if column_obj.name | lower == column_name | lower %}
            {% set column_monitors = elementary.column_monitors_by_type(column_obj.dtype, column_tests) %}
            {% set column_item = {'column': column_obj, 'monitors': column_monitors} %}
            {{ return(column_item) }}
        {% endif %}
    {% endfor %}

    {{ return(none) }}

{% endmacro %}

{% macro get_all_column_obj_and_monitors(model_relation, column_tests=none) %}

    {% set column_obj_and_monitors = [] %}
    {% set column_objects = adapter.get_columns_in_relation(model_relation) %}

    {% for column_obj in column_objects %}
        {% set column_monitors = elementary.column_monitors_by_type(column_obj.dtype, column_tests) %}
        {% set column_item = {'column': column_obj, 'monitors': column_monitors} %}
        {% do column_obj_and_monitors.append(column_item) %}
    {% endfor %}

    {{ return(column_obj_and_monitors) }}

{% endmacro %}

{% macro get_available_monitors() %}
    {% set monitors = elementary.get_config_var("edr_monitors").copy() %}
    {% set extended_column_numeric = monitors["column_numeric"].copy() %}
    {% do extended_column_numeric.append("sum") %}
    {% do monitors.update({"column_numeric": extended_column_numeric}) %}
    {% do return(monitors) %}
{% endmacro %}

{% macro column_monitors_by_type(data_type, column_tests=none) %}
    {% set monitors = column_tests or [] %}
    {% set normalized_data_type = elementary.normalize_data_type(data_type) %}

    {% set available_monitors = elementary.get_available_monitors() %}
    {% set available_all_types = available_monitors['column_any_type'] %}
    {% set available_numeric_monitors = available_monitors['column_numeric'] %}
    {% set available_string_monitors = available_monitors['column_string'] %}

    {% do monitors.extend(available_all_types) %}
    {% if normalized_data_type == 'numeric' %}
        {% do monitors.extend(available_numeric_monitors) %}
        {% set monitors = elementary.lists_intersection(monitors, available_numeric_monitors) %}
    {% elif normalized_data_type == 'string' %}
        {% do monitors.extend(available_string_monitors) %}
        {% set monitors = elementary.lists_intersection(monitors, available_string_monitors) %}
    {% endif %}
    {{ return(monitors | unique | list) }}
{% endmacro %}

{% macro all_column_monitors() %}
    {% set all_column_monitors = [] %}
    {% set available_monitors = elementary.get_available_monitors() %}
    {% do all_column_monitors.extend(available_monitors['column_any_type']) %}
    {% do all_column_monitors.extend(available_monitors['column_string']) %}
    {% do all_column_monitors.extend(available_monitors['column_numeric']) %}
    {{ return(all_column_monitors) }}
{% endmacro %}
