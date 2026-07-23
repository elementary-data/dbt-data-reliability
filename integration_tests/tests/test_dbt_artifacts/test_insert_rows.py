import json

from dbt_project import DbtProject


def _get_queries(dbt_project: DbtProject, **macro_args):
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_render_insert_rows_queries",
        macro_args=macro_args,
    )
    return json.loads(result[0])


def test_render_insert_rows_queries_values(dbt_project: DbtProject):
    queries = _get_queries(dbt_project)

    # Large default query_max_size / chunk_size -> everything fits in one query.
    assert len(queries) == 1
    query = queries[0]

    # String with a single quote is escaped and quoted.
    assert "'O''Brien'" in query
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
    queries = _get_queries(dbt_project, chunk_size=1)
    assert len(queries) == 2
    assert "'O''Brien'" in queries[0]
    assert "'second'" in queries[1]
