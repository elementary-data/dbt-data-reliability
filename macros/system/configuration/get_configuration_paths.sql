
{% macro schemas_configuration_table() %}

    {% set schemas_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['schemas']}}
    {% endset %}
    {{ return(schemas_monitoring_configuration) }}

{% endmacro %}


{% macro tables_configuration_table() %}

    {% set tables_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['tables']}}
    {% endset %}
    {{ return(tables_monitoring_configuration) }}

{% endmacro %}


{% macro columns_configuration_table() %}

    {% set columns_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['columns']}}
    {% endset %}
    {{ return(columns_monitoring_configuration) }}

{% endmacro %}