{% macro is_elementary_enabled() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema() %}
    {# Some adapters (e.g. Spark without a catalog) have no database but are still valid #}
    {% do return(database_name is not none or schema_name is not none) %}
{% endmacro %}
