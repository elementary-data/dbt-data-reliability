{% macro get_final_table_monitors(table_monitors_str) %}
    {%- set final_table_monitors = none %}
    {%- set default_table_monitors = elementary.get_default_table_monitors() %}
    {%- if table_monitors_str is not none %}
        {%- set configured_table_monitors = fromjson(table_monitors_str) %}
    {%- endif %}
    {%- if configured_table_monitors is defined and configured_table_monitors is not none and configured_table_monitors | length > 0 %}
        {%- set final_table_monitors = configured_table_monitors %}
    {%- else %}
        {%- set final_table_monitors = default_table_monitors %}
    {%- endif %}
    {# schema_changes is a different flow #}
    {% if 'schema_changes' in final_table_monitors %}
        {%- do final_table_monitors.remove('schema_changes') %}
    {% endif %}
    {{ return(final_table_monitors) }}
{% endmacro %}


{% macro get_default_table_monitors() %}

    {%- set default_table_monitors = elementary.get_config_var('edr_monitors')['table'] | list %}
    {{ return(default_table_monitors) }}

{% endmacro %}