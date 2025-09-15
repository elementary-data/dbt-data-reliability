import pytest
from dbt_project import DbtProject


@pytest.mark.only_on_targets(["athena"])
def test_athena_clean_elementary_test_tables_quoting(
    test_id: str, dbt_project: DbtProject
):
    """Test that Athena DROP TABLE statements use proper backtick quoting."""

    test_result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_athena_quoting_with_database", return_raw_edr_logs=True
    )

    assert any(
        "DROP TABLE IF EXISTS `test_db`.`test_schema`.`test_table`" in result
        for result in test_result
    ), "Expected DROP TABLE IF EXISTS with backtick quoting around identifiers"

    test_result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_athena_quoting_without_database",
        return_raw_edr_logs=True,
    )

    assert any(
        "DROP TABLE IF EXISTS `test_schema`.`test_table`" in result
        for result in test_result
    ), "Expected DROP TABLE IF EXISTS with backtick quoting around identifiers"

    test_result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_athena_quoting_special_chars", return_raw_edr_logs=True
    )

    assert any(
        "DROP TABLE IF EXISTS `test-db`.`test_schema`.`test-table`" in result
        for result in test_result
    ), "Expected DROP TABLE IF EXISTS with backtick quoting around identifiers with special characters"


@pytest.mark.only_on_targets(["athena"])
def test_athena_quoting_regression(test_id: str, dbt_project: DbtProject):
    """Regression test to ensure Athena quoting doesn't break existing functionality."""

    test_result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.test_athena_quoting_normal_names", return_raw_edr_logs=True
    )

    assert any(
        "DROP TABLE IF EXISTS `normal_db`.`normal_schema`.`normal_table`" in result
        for result in test_result
    ), "Expected DROP TABLE IF EXISTS with backtick quoting around normal identifiers"
