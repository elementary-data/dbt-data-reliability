{% macro test_render_insert_rows_queries(query_max_size=none, chunk_size=5000) %}
    {% set columns = [
        {"name": "name", "dtype": "varchar"},
        {"name": "int_col", "dtype": "integer"},
        {"name": "float_col", "dtype": "float"},
        {"name": "bool_col", "dtype": "boolean"},
        {"name": "json_col", "dtype": "varchar"},
        {"name": "null_col", "dtype": "varchar"},
        {"name": "created_at", "dtype": "timestamp"},
    ] %}
    {% set rows = [
        {
            "name": "O'Brien",
            "int_col": 42,
            "float_col": 3.14,
            "bool_col": true,
            "json_col": {"a": 1, "b": [1, 2]},
            "null_col": none,
        },
        {
            "name": "second",
            "int_col": 0,
            "float_col": -1.5,
            "bool_col": false,
            "json_col": [1, "x"],
            "null_col": "here",
        },
    ] %}
    {% set queries = elementary.get_insert_rows_queries(
        "my_table",
        columns,
        rows,
        query_max_size=query_max_size,
        chunk_size=chunk_size,
    ) %}
    {# Return the adapter-escaped literal alongside the queries so assertions can
       stay adapter-aware (escaping differs per warehouse, e.g. '' vs \'). #}
    {% do return(
        {
            "queries": queries,
            "escaped_quote_name": elementary.escape_special_chars("O'Brien"),
        }
    ) %}
{% endmacro %}
