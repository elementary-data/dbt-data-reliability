{% macro elementary_graph_defaults() %}
    {% do return(
        {
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
        }
    ) %}
{% endmacro %}

{% macro init_elementary_graph() %}
    {% do graph.setdefault("elementary", elementary.elementary_graph_defaults()) %}
{% endmacro %}
