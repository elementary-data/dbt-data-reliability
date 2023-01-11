{% macro get_final_table_monitors(table_anomalies) %}
    {%- set final_table_monitors = [] %}
    {%- set default_table_monitors = elementary.get_default_table_monitors() %}

    {%- if table_anomalies and table_anomalies | length > 0 %}
        {%- set final_table_monitors = elementary.lists_intersection(table_anomalies, default_table_monitors) %}
    {%- else %}
        {%- set final_table_monitors = default_table_monitors %}
    {%- endif %}
    {{ return(final_table_monitors) }}
{% endmacro %}


{% macro get_default_table_monitors() %}

    {%- set default_table_monitors = elementary.get_config_var('edr_monitors')['table'] | list %}
    {{ return(default_table_monitors) }}

{% endmacro %}