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

STRING_JINJA = r"{{ 'STRING' if (target.type == 'bigquery' or target.type == 'databricks') else 'character varying' if (target.type == 'redshift' or target.type == 'dremio') else 'TEXT' }}"


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
@pytest.mark.skip_targets(["databricks", "spark", "athena", "trino", "clickhouse"])
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
@pytest.mark.skip_targets(["databricks", "spark", "athena", "trino", "clickhouse"])
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
