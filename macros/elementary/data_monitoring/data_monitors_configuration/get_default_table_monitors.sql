{% macro get_default_table_monitors() %}

    {%- set default_table_monitors = var('edr_monitors')['table'] | list %}
    {{ return(default_table_monitors) }}

{% endmacro %}