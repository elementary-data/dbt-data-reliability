from typing import List

import pytest
from dbt_project import DbtProject

DATASET1 = [
    {"id": 1, "name": "Elon", "nick": "EGK"},
    {"id": 2, "name": "Itamar"},
    {"id": 3, "name": "Ofek"},
]

DATASET2 = [
    {"id": "a", "first_name": "Elon", "age": 22, "nick": "EGK"},
    {"id": "b", "first_name": "Itamar", "age": None},
    {"id": "c", "first_name": "Ofek", "age": None},
]

EXPECTED_FAILURES = [
    ("first_name", "column_added"),
    ("age", "column_added"),
    ("id", "type_changed"),
    ("name", "column_removed"),
]

STRING_JINJA = r"{{ 'STRING' if (target.type == 'bigquery' or target.type == 'databricks') else 'character varying' if (target.type == 'redshift' or target.type == 'dremio') else 'String' if target.type == 'clickhouse' else 'TEXT' }}"


def assert_test_results(test_results: List[dict]):
    expected_failures = EXPECTED_FAILURES.copy()
    failed_test_results = [
        test_result for test_result in test_results if test_result["status"] == "fail"
    ]
    assert len(failed_test_results) == len(expected_failures)

    for test_result in failed_test_results:
        test_failure = (
            test_result["column_name"].lower(),
            test_result["test_sub_type"].lower(),
        )
        expected_failures.remove(test_failure)
    assert not expected_failures


# Schema changes currently not supported on targets
# dbt-fusion caches column information and doesn't refresh when tables are recreated
@pytest.mark.skip_targets(["databricks", "spark", "athena", "trino"])
@pytest.mark.skip_for_dbt_fusion
def test_schema_changes(test_id: str, dbt_project: DbtProject):
    dbt_test_name = "elementary.schema_changes"
    test_result = dbt_project.test(test_id, dbt_test_name, data=DATASET1)
    assert test_result["status"] == "pass"
    test_results = dbt_project.test(
        test_id, dbt_test_name, data=DATASET2, multiple_results=True
    )
    assert_test_results(test_results)
    test_result = dbt_project.test(test_id, dbt_test_name)
    assert test_result["status"] == "pass"


# Schema changes currently not supported on targets
@pytest.mark.skip_targets(["databricks", "spark", "athena", "trino"])
def test_schema_changes_from_baseline(test_id: str, dbt_project: DbtProject):
    dbt_test_name = "elementary.schema_changes_from_baseline"
    test_results = dbt_project.test(
        test_id,
        dbt_test_name,
        test_args={"fail_on_added": True, "enforce_types": True},
        columns=[
            {"name": "id", "data_type": "integer"},
            {"name": "name", "data_type": STRING_JINJA},
            {"name": "nick", "data_type": STRING_JINJA},
        ],
        data=DATASET2,
        multiple_results=True,
    )
    assert_test_results(test_results)


POSTGRES_ALIASES_MODEL_SQL = """
select
    cast(1 as integer) as id,
    cast('2024-01-01 00:00:00' as timestamp) as updated_at,
    cast('12:00:00' as time) as opened_at,
    cast(1.5 as float) as score
"""


# Postgres reports these columns as "integer", "timestamp without time zone",
# "time without time zone" and "double precision". A baseline that declares
# them with their standard aliases should not report type changes.
@pytest.mark.only_on_targets(["postgres"])
@pytest.mark.parametrize("int_alias", ["int", "int4"])
def test_schema_changes_from_baseline_postgres_type_aliases(
    test_id: str, dbt_project: DbtProject, int_alias: str
):
    model_name = test_id.replace("[", "_").replace("]", "_")
    with dbt_project.create_temp_model_for_existing_table(
        model_name, materialization="table", raw_code=POSTGRES_ALIASES_MODEL_SQL
    ) as model_path:
        assert dbt_project.dbt_runner.run(select=str(model_path))

    test_results = dbt_project.test(
        test_id,
        "elementary.schema_changes_from_baseline",
        test_args={"fail_on_added": True, "enforce_types": True},
        columns=[
            {"name": "id", "data_type": int_alias},
            {"name": "updated_at", "data_type": "timestamp"},
            {"name": "opened_at", "data_type": "time"},
            {"name": "score", "data_type": "float"},
        ],
        as_model=True,
        multiple_results=True,
    )
    failures = [
        (result["column_name"], result["test_results_description"])
        for result in test_results
        if result["status"] != "pass"
    ]
    assert not failures
