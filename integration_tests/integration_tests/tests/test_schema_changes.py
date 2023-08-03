import pytest
from dbt_project import DbtProject

DBT_TEST_NAME = "elementary.schema_changes"


@pytest.mark.skip_targets(["databricks"])
def test_schema_changes(test_id: str, dbt_project: DbtProject):
    initial_data = [
        {"id": 1, "name": "Elon"},
        {"id": 2, "name": "Itamar"},
        {"id": 3, "name": "Ofek"},
    ]
    test_result = dbt_project.test(initial_data, test_id, DBT_TEST_NAME)
    assert test_result["status"] == "pass"
    changed_data = [
        {"id": 1, "name": "Elon", "age": 22},
        {"id": 2, "name": "Itamar", "age": None},
        {"id": 3, "name": "Ofek", "age": None},
    ]
    test_result = dbt_project.test(changed_data, test_id, DBT_TEST_NAME)
    assert test_result["status"] == "fail"
    test_result = dbt_project.test(changed_data, test_id, DBT_TEST_NAME)
    assert test_result["status"] == "pass"
