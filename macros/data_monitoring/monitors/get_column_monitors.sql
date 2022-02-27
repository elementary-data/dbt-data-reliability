{% macro column_monitors_by_type(data_type, config_monitors) %}

    {%- set converted_data_type = convert_data_type(data_type) %}

    {%- set all_types_monitors_except_schema = ['null_percent', 'null_count', 'unique'] | list %}
    {%- set numeric_monitors = ['min','max','zero_count','zero_percent', 'average', 'standard_deviation', 'variance'] | list %}
    {%- set string_monitors = ['min_length','max_length','missing_count','missing_percent'] | list %}

    {# If config_monitors is null, default is to use all relevant monitors for this data type #}
    {%- if config_monitors is defined and config_monitors|length %}
        {%- set monitors_list = config_monitors %}
    {%- else %}
        {%- set monitors_list = merge_lists([all_types_monitors_except_schema,numeric_monitors,string_monitors]) %}
    {%- endif %}

    {%- set column_monitors = [] %}

    {%- set all_types_intersect = lists_intersection(monitors_list, all_types_monitors_except_schema) %}
    {%- for monitor in all_types_intersect %}
        {{ column_monitors.append(monitor) }}
    {%- endfor %}

    {%- if converted_data_type == 'numeric' %}
        {%- set numeric_intersect = lists_intersection(monitors_list, numeric_monitors) %}
        {%- for monitor in numeric_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {%- if converted_data_type == 'string' %}
        {%- set string_intersect = lists_intersection(monitors_list, string_monitors) %}
        {%- for monitor in string_intersect %}
            {{ column_monitors.append(monitor) }}
        {%- endfor %}
    {%- endif %}

    {{ return(column_monitors) }}

{% endmacro %}
