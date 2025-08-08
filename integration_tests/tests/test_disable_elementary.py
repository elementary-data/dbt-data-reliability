from dbt_project import DbtProject

COLUMN_NAME = "some_column"


def test_running_dbt_tests_without_elementary(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: "hello"}]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        elementary_enabled=False,
    )
    assert test_result["status"] == "pass"
