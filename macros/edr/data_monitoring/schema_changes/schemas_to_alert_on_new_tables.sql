{% macro schemas_to_alert_on_new_tables() %}
    {%- set schemas_list = elementary.get_config_var('schemas_to_alert_on_new_tables') %}
    {%- set upper_schemas_list = [] %}
    {%- for schema in schemas_list %}
        {%- do upper_schemas_list.append(schema.upper()) %}
    {%- endfor %}
    {%- set alert_on_schema_changes = elementary.strings_list_to_tuple(upper_schemas_list) %}
    {{- return(alert_on_schema_changes) }}
{% endmacro %}