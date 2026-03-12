{% macro clear_env() %}
    {% set database_name, schema_name = elementary.get_package_database_and_schema(
        "elementary"
    ) %}
    {% do elementary_tests.edr_drop_schema(database_name, schema_name) %}
    {% do elementary_tests.edr_drop_schema(
        elementary.target_database(), generate_schema_name()
    ) %}
{% endmacro %}

{% macro drop_test_schemas(num_workers=8) %}
    {#
      Drop every schema that a CI test run may have created.
      This covers the base schema (no xdist suffix) as well as
      each pytest-xdist worker schema (_gw0 … _gw<N-1>).
      Called from the workflow with `if: always()` so that schemas
      are cleaned up even when the pytest process is cancelled or
      crashes before its own teardown runs.
    #}
    {% set database = elementary.target_database() %}
    {% set base_schema = target.schema %}
    {% set suffixes = [""] %}
    {% for i in range(num_workers) %} {% do suffixes.append("_gw" ~ i) %} {% endfor %}

    {% for suffix in suffixes %}
        {% set test_schema = base_schema ~ suffix %}
        {% set elementary_schema = base_schema ~ "_elementary" ~ suffix %}
        {% do log(
            "Dropping schemas: " ~ test_schema ~ ", " ~ elementary_schema,
            info=true,
        ) %}
        {% do elementary_tests.edr_drop_schema(database, elementary_schema) %}
        {% do elementary_tests.edr_drop_schema(database, test_schema) %}
    {% endfor %}
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

{% macro vertica__edr_drop_schema(database_name, schema_name) %}
    {#- Vertica DDL is auto-committed; an explicit adapter.commit() would
        fail with "no transaction in progress". -#}
    {% do run_query("DROP SCHEMA IF EXISTS " ~ schema_name ~ " CASCADE") %}
{% endmacro %}
