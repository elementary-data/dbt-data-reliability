{% macro check_schema_exists(database_name, schema_name) %}
    {%- set schemas_list = [] %}
    {% if execute %}
        {%- set get_schemas = dbt.list_schemas(database_name) %}
        {%- set results_rows = get_schemas.rows.values() %}
        {%- if results_rows %}
            {% for result_row in results_rows %}
                {{ schemas_list.append(result_row.values()[1]) }}
            {% endfor %}
        {%- endif %}
    {% endif %}

    {%- if schema_name in schemas_list %}
        {{ return(true) }}
    {%- else %}
        {{ return(false) }}
    {%- endif %}
{% endmacro %}