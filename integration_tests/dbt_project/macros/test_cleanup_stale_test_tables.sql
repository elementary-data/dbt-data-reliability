{% macro test_cleanup_stale_test_tables() %}
    {% set elementary_database, elementary_schema = (
        elementary.get_package_database_and_schema()
    ) %}

    {% set fake_tables = [
        "test_cleanup__tmp_0000001",
        "test_cleanup_alerts__tmp_0000002",
        "test_cleanup_extra__tmp_0000003",
    ] %}
    {% for table_name in fake_tables %}
        {% set relation = api.Relation.create(
            database=elementary_database,
            schema=elementary_schema,
            identifier=table_name,
            type="table",
        ) %}
        {% do elementary.create_or_replace(false, relation, "select 1 as id") %}
    {% endfor %}

    {% set table_name_pattern = "test%__tmp_%" %}

    {# Verify all 3 exist before cleanup #}
    {% set tables_before = elementary.get_stale_test_tables(
        elementary_database, elementary_schema, 0, table_name_pattern, 2000
    ) %}

    {# Delete only 2 out of 3 via limit=2 #}
    {% do elementary.cleanup_stale_test_tables(hours=0, limit=2) %}

    {# Verify exactly 1 remains #}
    {% set tables_after = elementary.get_stale_test_tables(
        elementary_database, elementary_schema, 0, table_name_pattern, 2000
    ) %}

    {% do return(
        {
            "tables_before_count": tables_before | length,
            "tables_after_count": tables_after | length,
        }
    ) %}
{% endmacro %}
