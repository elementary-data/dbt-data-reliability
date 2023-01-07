{% macro get_dbt_artifacts_materialized() %}
    {{ return(adapter.dispatch('get_dbt_artifacts_materialized', 'elementary')()) }}
{% endmacro %}

{% macro default__get_dbt_artifacts_materialized() %}
    {# Incremental - to avoid dropping and creating the table #}
    {% do return('incremental') %}
{% endmacro %}

{% macro spark__get_dbt_artifacts_materialized() %}
    {# Returning table here mainly because this was the materialization of artifacts before,
       and changing the materialization will not work because it changes the table type to delta tables #}
    {% do return('table') %}
{% endmacro %}
