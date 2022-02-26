
{% macro get_configuration_path() %}

    {% set schemas_configuration_path %}
        {{ target.database ~"."~ target.schema ~"."~ var('monitoring_configuration')}}
    {% endset %}
    {{ return(schemas_configuration_path) }}

{% endmacro %}