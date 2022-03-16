{% macro get_final_table_monitors(table_tests=none) %}
    {%- set final_table_monitors = [] %}
    {%- set default_table_monitors = elementary.get_default_table_monitors() %}

    {%- if table_tests is defined and table_tests is not none and table_tests | length > 0 %}
        {%- set final_table_monitors = elementary.lists_intersection(table_tests,default_table_monitors) %}
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