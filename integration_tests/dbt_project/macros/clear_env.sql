{% macro clear_env() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema(
        "elementary"
    ) %}
    {% do elementary_tests.edr_drop_schema(database_name, schema_name) %}
    {% do elementary_tests.edr_drop_schema(
        elementary.target_database(), generate_schema_name()
    ) %}
{% endmacro %}

{% macro edr_drop_schema(database_name, schema_name) %}
    {% do return(
        adapter.dispatch("edr_drop_schema", "elementary_tests")(
            database_name, schema_name
        )
    ) %}
{% endmacro %}

{% macro default__edr_drop_schema(database_name, schema_name) %}
    {% set schema_relation = api.Relation.create(
        database=database_name, schema=schema_name
    ) %}
    {% do dbt.drop_schema(schema_relation) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro clickhouse__edr_drop_schema(database_name, schema_name) %}
    {% do run_query("DROP DATABASE IF EXISTS " ~ schema_name) %}
    {% do adapter.commit() %}
{% endmacro %}

{% macro spark__edr_drop_schema(database_name, schema_name) %}
    {% set safe_schema = schema_name | replace("`", "``") %}
    {% do run_query("DROP DATABASE IF EXISTS `" ~ safe_schema ~ "` CASCADE") %}
{% endmacro %}

{% macro athena__edr_drop_schema(database_name, schema_name) %}
    {#
      Athena's SQL `DROP SCHEMA … CASCADE` can fail when the schema
      contains Iceberg tables.  Work around this by first dropping every
      relation individually (the adapter handles Iceberg vs Hive
      differences in its drop_relation implementation) and then removing
      the now-empty schema.
    #}
    {% set schema_relation = api.Relation.create(
        database=database_name, schema=schema_name
    ) %}
    {% set relations = adapter.list_relations_without_caching(schema_relation) %}
    {% for relation in relations %}
        {% do adapter.drop_relation(relation) %}
    {% endfor %}
    {% do dbt.drop_schema(schema_relation) %}
{% endmacro %}

{% macro duckdb__edr_drop_schema(database_name, schema_name) %}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ schema_name ~ " CASCADE") %}
    {% do adapter.commit() %}
{% endmacro %}
