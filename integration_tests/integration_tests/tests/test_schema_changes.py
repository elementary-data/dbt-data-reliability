from typing import List

import pytest
from dbt_project import DbtProject

DATASET1 = [
    {"id": 1, "name": "Elon"},
    {"id": 2, "name": "Itamar"},
    {"id": 3, "name": "Ofek"},
]

DATASET2 = [
    {"id": "a", "first_name": "Elon", "age": 22},
    {"id": "b", "first_name": "Itamar", "age": None},
    {"id": "c", "first_name": "Ofek", "age": None},
]

EXPECTED_FAILURES = [
    "column_added",  # first_name
    "column_added",  # age
    "type_changed",  # id
    "column_removed",  # name
]


def assert_test_results(test_results: List[dict]):
    expected_failures = EXPECTED_FAILURES.copy()
    for test_result in test_results:
        test_sub_type = test_result["test_sub_type"]
        assert test_result["status"] == "fail" and test_sub_type in expected_failures
        expected_failures.remove(test_sub_type)
    assert not expected_failures


@pytest.mark.skip_targets(["databricks"])
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


@pytest.mark.skip_targets(["databricks"])
def test_schema_changes_from_baseline(test_id: str, dbt_project: DbtProject):
    dbt_test_name = "elementary.schema_changes_from_baseline"
    test_results = dbt_project.test(
        test_id,
        dbt_test_name,
        test_args={"fail_on_added": True, "enforce_types": True},
        columns=[
            {"name": "id", "data_type": "integer"},
            {"name": "name", "data_type": "string"},
        ],
        data=DATASET2,
        multiple_results=True,
    )
    assert_test_results(test_results)
