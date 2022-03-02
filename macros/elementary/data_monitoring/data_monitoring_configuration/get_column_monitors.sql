{% macro column_monitors_by_type(data_type, config_monitors) %}

    {%- set normalized_data_type = elementary.normalize_data_type(data_type) %}

    {%- set default_all_types = var('edr_monitors')['column_any_type'] | list %}
    {%- set default_numeric_monitors = var('edr_monitors')['column_numeric'] | list %}
    {%- set default_string_monitors = var('edr_monitors')['column_string'] | list %}

    {# If config_monitors is null, default is to use all relevant monitors for this data type #}
    {%- if config_monitors is defined and config_monitors is not none and config_monitors | length > 0 %}
        {%- set monitors_list = config_monitors %}
    {%- else %}
        {% set monitors_list = [] %}
        {% do monitors_list.extend(default_all_types) %}
        {% do monitors_list.extend(numeric_monitors) %}
        {% do monitors_list.extend(string_monitors) %}
    {%- endif %}

    {%- set column_monitors = [] %}

    {%- set all_types_intersect = elementary.lists_intersection(monitors_list, default_all_types) %}
    {%- for monitor in all_types_intersect %}
        {{ column_monitors.append(monitor) }}
    {%- endfor %}

    {%- if normalized_data_type == 'numeric' %}
        {%- set numeric_intersect = elementary.lists_intersection(monitors_list, default_numeric_monitors) %}
        {%- for monitor in numeric_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {%- if normalized_data_type == 'string' %}
        {%- set string_intersect = elementary.lists_intersection(monitors_list, default_string_monitors) %}
        {%- for monitor in string_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {{ return(column_monitors) }}

{% endmacro %}
