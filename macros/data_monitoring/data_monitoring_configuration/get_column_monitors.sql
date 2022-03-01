{% macro column_monitors_by_type(data_type, config_monitors) %}

    {%- set converted_data_type = elementary.convert_data_type(data_type) %}

    {%- set all_types_monitors_except_schema = var('edr_monitors')['column_any_type'] | list %}
    {%- set numeric_monitors = var('edr_monitors')['column_numeric'] | list %}
    {%- set string_monitors = var('edr_monitors')['column_string'] | list %}

    {# If config_monitors is null, default is to use all relevant monitors for this data type #}
    {%- if config_monitors is defined and config_monitors|length %}
        {%- set monitors_list = config_monitors %}
    {%- else %}
        {%- set monitors_list = elementary.merge_lists([all_types_monitors_except_schema,numeric_monitors,string_monitors]) %}
    {%- endif %}

    {%- set column_monitors = [] %}

    {%- set all_types_intersect = elementary.lists_intersection(monitors_list, all_types_monitors_except_schema) %}
    {%- for monitor in all_types_intersect %}
        {{ column_monitors.append(monitor) }}
    {%- endfor %}

    {%- if converted_data_type == 'numeric' %}
        {%- set numeric_intersect = elementary.lists_intersection(monitors_list, numeric_monitors) %}
        {%- for monitor in numeric_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {%- if converted_data_type == 'string' %}
        {%- set string_intersect = elementary.lists_intersection(monitors_list, string_monitors) %}
        {%- for monitor in string_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {{ return(column_monitors) }}

{% endmacro %}
