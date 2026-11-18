{% macro init_elementary_graph() %}
    {% set defaults = {
        "elementary_test_results": {},
        "elementary_test_failed_row_counts": {},
        "tests_schema_name": none,
        "tables": {
            "metrics": {"relations": [], "rows": []},
            "schema_snapshots": [],
        },
        "temp_test_table_relations_map": {},
        "duration_context_stack": {},
        "microbatch_compiled_code_by_unique_id": {},
    } %}
    {#- Each entry is filled on its own: a cache read before this hook (dbt v2 shares
        `graph` between parsing and the run) can leave an empty "elementary" mapping,
        which a single setdefault on the whole mapping would keep as it is. -#}
    {% set elementary_graph = graph.setdefault("elementary", {}) %}
    {% for entry, default in defaults.items() %}
        {% do elementary_graph.setdefault(entry, default) %}
    {% endfor %}
{% endmacro %}
