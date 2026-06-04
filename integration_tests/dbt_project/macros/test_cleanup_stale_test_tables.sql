{% macro test_cleanup_stale_test_tables() %}
    {% set elementary_database, elementary_schema = (
        elementary.get_package_database_and_schema()
    ) %}

    {# Create fake temp tables matching the elementary temp table naming pattern #}
    {% set fake_tables = [
        "test_aaa000_elementary_source_schema_changes_cleanup_test_source___schema_changes__tmp_20200101120000000000001",
        "test_aaa000_elementary_source_schema_changes_cleanup_test_source___schema_changes_alerts__tmp_20200101120000000000002",
    ] %}
    {% for table_name in fake_tables %}
        {% set _, relation = dbt.get_or_create_relation(
            database=elementary_database,
            schema=elementary_schema,
            identifier=table_name,
            type="table",
        ) %}
        {% do elementary.create_or_replace(false, relation, "select 1 as id") %}
    {% endfor %}

    {# Assert tables exist before cleanup #}
    {% set table_name_pattern = "test%__tmp_%" %}
    {% set tables_before = elementary.get_stale_test_tables(
        elementary_database, elementary_schema, 0, table_name_pattern
    ) %}

    {# Run cleanup #}
    {% do elementary.cleanup_stale_test_tables(hours=0) %}

    {# Assert tables are gone after cleanup #}
    {% set tables_after = elementary.get_stale_test_tables(
        elementary_database, elementary_schema, 0, table_name_pattern
    ) %}

    {% do return(
        {
            "tables_before_count": tables_before | length,
            "tables_after_count": tables_after | length,
        }
    ) %}
{% endmacro %}
