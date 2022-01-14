
{% macro schemas_configuration_table() %}

    {% set schemas_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['schemas_monitoring_configuration']}}
    {% endset %}
    {{ return(schemas_monitoring_configuration) }}

{% endmacro %}


{% macro tables_configuration_table() %}

    {% set tables_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['tables_monitoring_configuration']}}
    {% endset %}
    {{ return(tables_monitoring_configuration) }}

{% endmacro %}


{% macro columns_configuration_table() %}

    {% set columns_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['columns_monitoring_configuration']}}
    {% endset %}
    {{ return(columns_monitoring_configuration) }}

{% endmacro %}