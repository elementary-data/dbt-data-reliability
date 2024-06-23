{% macro get_column_obj_and_monitors(model_relation, column_name, monitors=none) %}

    {% set column_obj_and_monitors = [] %}
    {% set column_objects = adapter.get_columns_in_relation(model_relation) %}
    {% for column_obj in column_objects %}
        {% if column_obj.name.strip('"') | lower == column_name.strip('"') | lower %}
            {% set column_monitors = elementary.column_monitors_by_type(elementary.get_column_data_type(column_obj), monitors) %}
            {% set column_item = {'column': column_obj, 'monitors': column_monitors} %}
            {{ return(column_item) }}
        {% endif %}
    {% endfor %}

    {{ return(none) }}

{% endmacro %}

{% macro get_all_column_obj_and_monitors(model_relation, monitors=none) %}

    {% set column_obj_and_monitors = [] %}
    {% set column_objects = adapter.get_columns_in_relation(model_relation) %}

    {% for column_obj in column_objects %}
        {% set column_monitors = elementary.column_monitors_by_type(elementary.get_column_data_type(column_obj), monitors) %}
        {% set column_item = {'column': column_obj, 'monitors': column_monitors} %}
        {% do column_obj_and_monitors.append(column_item) %}
    {% endfor %}

    {{ return(column_obj_and_monitors) }}

{% endmacro %}

{% macro column_monitors_by_type(data_type, chosen_monitors=none) %}
    {% set normalized_data_type = elementary.normalize_data_type(data_type) %}
    {% set monitors = [] %}
    {% set chosen_monitors = chosen_monitors or elementary.get_agg_column_monitors(only_defaults=true) %}
    {% set available_monitors = elementary.get_available_monitors() %}

    {% set any_type_monitors = elementary.lists_intersection(chosen_monitors, available_monitors["column_any_type"]) %}
    {% do monitors.extend(any_type_monitors) %}
    {% if normalized_data_type == 'numeric' %}
        {% set numeric_monitors = elementary.lists_intersection(chosen_monitors, available_monitors["column_numeric"]) %}
        {% do monitors.extend(numeric_monitors) %}
    {% elif normalized_data_type == 'string' %}
        {% set string_monitors = elementary.lists_intersection(chosen_monitors, available_monitors["column_string"]) %}
        {% do monitors.extend(string_monitors) %}
    {% elif normalized_data_type == 'boolean' %}
        {% set boolean_monitors = elementary.lists_intersection(chosen_monitors, available_monitors["column_boolean"]) %}
        {% do monitors.extend(boolean_monitors) %}
    {% endif %}
    {{ return(monitors | unique | list) }}
{% endmacro %}

{% macro get_agg_column_monitors(only_defaults=false) %}
    {% set agg_column_monitors = [] %}
    {% if only_defaults %}
        {% set monitors = elementary.get_default_monitors() %}
    {% else %}
        {% set monitors = elementary.get_available_monitors() %}
    {% endif %}
    {% do agg_column_monitors.extend(monitors['column_any_type']) %}
    {% do agg_column_monitors.extend(monitors['column_string']) %}
    {% do agg_column_monitors.extend(monitors['column_numeric']) %}
    {% do agg_column_monitors.extend(monitors['column_boolean']) %}
    {{ return(agg_column_monitors) }}
{% endmacro %}
