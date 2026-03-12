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
{% endmacro %}
