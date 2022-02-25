{% macro get_table_monitors(config_monitors) %}

    {%- set all_table_monitors_except_schema %}
        [
        'row_count'
        ]
    {% endset %}

    {%- set table_monitors = [] %}

    {% set monitors_intersect = lists_intersection(config_monitors, all_table_monitors_except_schema) %}
    {% for monitor in monitors_intersect %}
        {{ table_monitors.append(monitor) }}
    {% endfor %}

    {{ return(table_monitors) }}

{% endmacro %}

