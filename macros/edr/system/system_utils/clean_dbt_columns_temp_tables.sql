{% macro clean_dbt_columns_temp_tables() %}
    {% do elementary.edr_log("Deleting dbt_columns temp tables") %}
    {% set elementary_database, elementary_schema = elementary.get_package_database_and_schema() %}
    {% set dbt_columns_temp_tables_relations = dbt_utils.get_relations_by_prefix(schema=elementary_schema, prefix='dbt_columns__tmp_', database=elementary_database) %}
    {% for temp_relation in dbt_columns_temp_tables_relations %}
        {% do elementary.edr_log("Deleting temp table - " ~ temp_relation) %}
        {% do adapter.drop_relation(temp_relation) %}
    {% endfor %}
{% endmacro %}
