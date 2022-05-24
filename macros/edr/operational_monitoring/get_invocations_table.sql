-- TODO: Can we validate table exists?

{% macro get_invocations_table() %}
    {%- set database_name, schema_name = elementary.get_package_database_and_schema('elementary') %}
    {%- set invocation_times_table = adapter.get_relation(database=database_name,
                                                        schema=schema_name,
                                                        identifier='invocation_times') %}
    {{ return(invocation_times_table) }}
{% endmacro %}