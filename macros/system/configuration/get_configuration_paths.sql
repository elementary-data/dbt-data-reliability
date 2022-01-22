
{% macro configured_schemas_path() %}

    {% set schemas_configuration_path %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['schemas']}}
    {% endset %}
    {{ return(schemas_configuration_path) }}

{% endmacro %}


{% macro configured_tables_path() %}

    {% set tables_configuration_path %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['tables']}}
    {% endset %}
    {{ return(tables_configuration_path) }}

{% endmacro %}


{% macro configured_columns_path() %}

    {% set columns_configuration_path %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')['columns']}}
    {% endset %}
    {{ return(columns_configuration_path) }}

{% endmacro %}