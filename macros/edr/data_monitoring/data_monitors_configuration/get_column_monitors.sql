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


{% macro column_monitors_by_type(data_type, column_tests=none) %}
    {% set monitors = column_tests or [] %}
    {% set normalized_data_type = elementary.normalize_data_type(data_type) %}

    {% set default_monitors = elementary.get_config_var('edr_monitors') %}
    {% set default_all_types = default_monitors['column_any_type'] %}
    {% set default_numeric_monitors = default_monitors['column_numeric'] %}
    {% set default_string_monitors = default_monitors['column_string'] %}

    {% do monitors.extend(default_all_types) %}
    {% if normalized_data_type == 'numeric' %}
        {% do monitors.extend(default_numeric_monitors) %}
    {% elif normalized_data_type == 'string' %}
        {% do monitors.extend(default_string_monitors) %}
    {% endif %}
    {{ return(monitors | unique | list) }}
{% endmacro %}

{% macro all_column_monitors() %}
    {% set all_column_monitors = [] %}
    {% set default_monitors = elementary.get_config_var('default_monitors') %}
    {% do all_column_monitors.extend(default_monitors['column_any_type']) %}
    {% do all_column_monitors.extend(default_monitors['column_string']) %}
    {% do all_column_monitors.extend(default_monitors['column_numeric']) %}
    {{ return(all_column_monitors) }}
{% endmacro %}
