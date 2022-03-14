{% macro schema_exists_in_target(schema_name) %}
    {%- set full_schema_name = elementary.target_database() ~'.'~ schema_name %}
    {%- set query_info_schema = elementary.get_tables_from_information_schema(full_schema_name) %}
    {%- set results = elementary.result_column_to_list(query_info_schema) %}
    {%- if results %}
        {{ return(true) }}
    {%- else %}
        {{ return(false) }}
    {%- endif %}
{% endmacro %}