{% macro schemas_for_alert_on_schema_changes() %}
    {%- set schemas_list = var('schemas_for_alert_on_schema_changes') %}
    {%- set upper_schemas_list = [] %}
    {%- for schema in schemas_list %}
        {%- do upper_schemas_list.append(schema.upper()) %}
    {%- endfor %}
    {%- set alert_on_schema_changes = strings_list_to_tuple(upper_schemas_list) %}
    {{- return(alert_on_schema_changes) }}
{% endmacro %}