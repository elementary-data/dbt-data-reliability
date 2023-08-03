import pytest
from dbt_project import DbtProject


@pytest.mark.skip_targets(["databricks"])
def test_schema_changes(test_id: str, dbt_project: DbtProject):
    dbt_test_name = "elementary.schema_changes"
    initial_data = [
        {"id": 1, "name": "Elon"},
        {"id": 2, "name": "Itamar"},
        {"id": 3, "name": "Ofek"},
    ]
    test_result = dbt_project.test(initial_data, test_id, dbt_test_name)
    assert test_result["status"] == "pass"
    changed_data = [
        {"id": "a", "first_name": "Elon", "age": 22},
        {"id": "b", "first_name": "Itamar", "age": None},
        {"id": "c", "first_name": "Ofek", "age": None},
    ]
    test_results = dbt_project.test(
        changed_data, test_id, dbt_test_name, multiple_results=True
    )
    expected_failures = [
        "column_added",  # first_name
        "column_added",  # age
        "type_changed",  # id
        "column_removed",  # name
    ]
    for test_result in test_results:
        test_sub_type = test_result["test_sub_type"]
        assert test_result["status"] == "fail" and test_sub_type in expected_failures

        expected_failures.remove(test_sub_type)
    assert not expected_failures

    test_result = dbt_project.test(changed_data, test_id, dbt_test_name)
    assert test_result["status"] == "pass"
