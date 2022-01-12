
{% macro get_schemas_configuration() %}

    {% set schemas_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['schemas_monitoring_configuration']}}
    {% endset %}
    {{ return(schemas_monitoring_configuration) }}

{% endmacro %}


{% macro get_tables_configuration() %}

    {% set tables_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['tables_monitoring_configuration']}}
    {% endset %}
    {{ return(tables_monitoring_configuration) }}

{% endmacro %}


{% macro get_columns_configuration() %}

    {% set columns_monitoring_configuration %}
        {{ target.database ~"."~ target.schema ~"."~ var('elementary')['columns_monitoring_configuration']}}
    {% endset %}
    {{ return(columns_monitoring_configuration) }}

{% endmacro %}