{% macro get_columns_and_types(table_name, schema_name = none, database_name = none) %}

    {# dbt models can be found with identifier only #}
    {# for non-dbt tables database_name and schema_name are required #}

    {%- if not database_name %}
        {%- set database_name = elementary.target_database() %}
    {%- endif %}
    {%- if not schema_name %}
        {%- set schema_name = target.schema %}
    {%- endif %}

    {%- set columns = [] %}

    {%- set relation = adapter.get_relation(
          database=database_name,
          schema=schema_name,
          identifier=table_name) -%}

    {%- set columns_from_relation = adapter.get_columns_in_relation(relation) -%}

    {% for column in columns_from_relation %}
        {%- set column_item = {'column_name': column['column'], 'data_type': elementary.normalize_data_type(elementary.get_column_data_type(column))} %}
        {%- do columns.append(column_item) -%}
    {% endfor %}

    {{ return(columns) }}

{% endmacro %}