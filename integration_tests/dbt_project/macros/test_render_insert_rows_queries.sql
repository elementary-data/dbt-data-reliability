{% macro test_render_insert_rows_queries(
    columns, rows, escape_sample, query_max_size=none, chunk_size=5000
) %}
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
            "escaped_quote_name": elementary.escape_special_chars(
                escape_sample
            ),
        }
    ) %}
{% endmacro %}
