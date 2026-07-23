import json

from dbt_project import DbtProject


def _render(dbt_project: DbtProject, **macro_args):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_render_insert_rows_queries",
        macro_args=macro_args,
    )
    output = json.loads(result[0])
    # Escaping differs per adapter ('' vs \'), so derive the expected quoted
    # literal from the adapter instead of hardcoding it.
    name_literal = "'{}'".format(output["escaped_quote_name"])
    return output["queries"], name_literal


def test_render_insert_rows_queries_values(dbt_project: DbtProject):
    queries, name_literal = _render(dbt_project)

    # Large default query_max_size / chunk_size -> everything fits in one query.
    assert len(queries) == 1
    query = queries[0]

    # String with a single quote is escaped (adapter-specific) and quoted.
    assert name_literal in query
    # Numbers render without quotes.
    assert "42" in query
    assert "-1.5" in query
    # NULL for a none value, rendered unquoted.
    assert "null" in query.lower()
    # Nested mapping / sequence serialized via tojson into a quoted string.
    assert '\'{"a": 1, "b": [1, 2]}\'' in query
    assert "'[1, \"x\"]'" in query
    # Both rows are present, joined into a single VALUES list.
    assert query.count("'second'") == 1


def test_render_insert_rows_queries_chunking(dbt_project: DbtProject):
    # chunk_size=1 forces each row into its own query.
    queries, name_literal = _render(dbt_project, chunk_size=1)
    assert len(queries) == 2
    assert name_literal in queries[0]
    assert "'second'" in queries[1]
