{% macro get_final_table_monitors(monitors=none) %}
    {%- set final_table_monitors = [] %}

    {%- if monitors and monitors | length > 0 %}
        {%- set allowed_table_monitors = elementary.get_allowed_table_monitors() %}
        {%- set final_table_monitors = elementary.lists_intersection(monitors, allowed_table_monitors) %}
    {%- else %}
        {%- set final_table_monitors = elementary.get_default_table_monitors() %}
    {%- endif %}
    {{ return(final_table_monitors) }}
{% endmacro %}


{% macro get_default_table_monitors() %}
    {%- set default_table_monitors = elementary.get_config_var('edr_monitors')['table'] | list %}
    {{ return(default_table_monitors) }}
{% endmacro %}


{% macro get_allowed_table_monitors() %}
    {% do return(["row_count", "freshness", "event_freshness"]) %}
{% endmacro %}