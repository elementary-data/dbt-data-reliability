{% macro insert_sentinel_row(table_name, sentinel_alias) %}
    {#- Insert a sentinel row into the given Elementary table.
        Used by integration tests to verify that replace_table_data
        actually truncates the whole table (sentinel disappears) rather
        than doing a diff-based update (sentinel would survive). -#}
    {% set relation = ref(table_name) %}
    {% do run_query(
        "INSERT INTO " ~ relation ~ " (unique_id, alias, name)"
        " VALUES ('test.sentinel', '" ~ sentinel_alias ~ "', 'sentinel')"
    ) %}
    {#- Use raw SQL COMMIT instead of adapter.commit() because some adapters
        (e.g. Vertica) raise "no transaction in progress" from adapter.commit()
        within a run_operation context. A raw COMMIT is harmless when there is
        no open transaction (most databases treat it as a no-op). -#}
    {% do run_query("COMMIT") %}
{% endmacro %}
