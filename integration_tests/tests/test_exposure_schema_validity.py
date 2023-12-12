from typing import List

import pytest
from dbt_project import DbtProject

DBT_TEST_NAME = "elementary.exposure_schema_validity"


def seed(dbt_project: DbtProject):
    seed_result = dbt_project.dbt_runner.seed(full_refresh=True)
    assert seed_result is True


def validate_failing_query_output(
    dbt_dir: str,
    test_output: str,
    table_name: str,
    expected_content_strings: List[str],
):
    generated_query = f"target/compiled/elementary_tests/models/schema.yml/elementary_exposure_schema_validity_{table_name}_.sql"
    assert generated_query in test_output
    output_file = f"{dbt_dir}/{generated_query}"
    with open(output_file) as f:
        generated_query_content = f.read()
        for content_string_to_validate in expected_content_strings:
            assert content_string_to_validate in generated_query_content


def test_exposure_schema_validity_existing_exposure_yml_invalid(
    test_id: str, dbt_project: DbtProject
):
    seed(dbt_project)
    run_result = dbt_project.dbt_runner.run(
        models="orders", full_refresh=True, quiet=True
    )
    assert run_result is True
    test_result, test_output = dbt_project.dbt_runner._run_command(
        command_args=["test", "-s", "tag:exposure_orders"],
        log_format="text",
        capture_output=True,
        quiet=True,
        log_output=False,
    )
    assert test_result is False
    validate_failing_query_output(
        dbt_project.project_dir_path,
        test_output,
        "orders",
        [
            "different data type for the column order_id string vs",
            "ZOMG column missing in the model",
        ],
    )


def test_exposure_schema_validity_existing_exposure_yml_valid(
    test_id: str, dbt_project: DbtProject
):
    seed(dbt_project)
    run_result = dbt_project.dbt_runner.run(
        models="customers", full_refresh=True, quiet=True
    )
    assert run_result is True
    test_result, test_output = dbt_project.dbt_runner._run_command(
        command_args=["test", "-s", "tag:exposure_customers"],
        capture_output=True,
        quiet=True,
        log_output=False,
    )
    assert test_result is True


@pytest.mark.skip_targets(["spark"])
def test_exposure_schema_validity_no_exposures(test_id: str, dbt_project: DbtProject):
    test_result = dbt_project.test(test_id, DBT_TEST_NAME)
    assert test_result["status"] == "pass"


@pytest.mark.skip_targets(["spark"])
def test_exposure_schema_validity_correct_columns_and_types(
    test_id: str, dbt_project: DbtProject
):
    explicit_target_for_bigquery = (
        "other"
        if dbt_project.dbt_runner.target in ["bigquery", "snowflake", ""]
        else "string"
    )
    DBT_TEST_ARGS = {
        "node": "models.exposures_test",
        "columns": [{"name": "order_id", "dtype": "string", "data_type": "string"}],
        "exposures": {
            "ZOMG": {
                "meta": {
                    "referenced_columns": [
                        {
                            "column_name": "order_id",
                            "data_type": explicit_target_for_bigquery,
                        }
                    ]
                },
                "url": "http://bla.com",
                "name": "ZOMG",
                "depends_on": {"nodes": ["models.exposures_test"]},
            }
        },
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, columns=[dict(name="bla")], as_model=True
    )
    assert test_result["status"] == "pass"


@pytest.mark.skip_targets(["spark"])
def test_exposure_schema_validity_correct_columns_and_invalid_type(
    test_id: str, dbt_project: DbtProject
):
    DBT_TEST_ARGS = {
        "node": "models.exposures_test",
        "columns": [{"name": "order_id", "dtype": "numeric", "data_type": "numeric"}],
        "exposures": {
            "ZOMG": {
                "meta": {
                    "referenced_columns": [
                        {"column_name": "order_id", "data_type": "string"}
                    ]
                },
                "url": "http://bla.com",
                "name": "ZOMG",
                "depends_on": {"nodes": ["models.exposures_test"]},
            }
        },
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, columns=[dict(name="bla")], as_model=True
    )

    assert (
        "different data type for the column order_id string vs"
        in test_result["test_results_query"]
    )
    assert test_result["status"] == "fail"


@pytest.mark.skip_targets(["spark"])
def test_exposure_schema_validity_correct_columns_and_missing_type(
    test_id: str, dbt_project: DbtProject
):
    DBT_TEST_ARGS = {
        "node": "models.exposures_test",
        "columns": [{"name": "order_id", "dtype": "numeric", "data_type": "numeric"}],
        "exposures": {
            "ZOMG": {
                "meta": {"referenced_columns": [{"column_name": "order_id"}]},
                "url": "http://bla.com",
                "name": "ZOMG",
                "depends_on": {"nodes": ["models.exposures_test"]},
            }
        },
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, columns=[dict(name="bla")], as_model=True
    )

    assert test_result["status"] == "pass"


@pytest.mark.skip_targets(["spark"])
def test_exposure_schema_validity_missing_columns(
    test_id: str, dbt_project: DbtProject
):
    DBT_TEST_ARGS = {
        "node": "models.exposures_test",
        "columns": [{"name": "order", "dtype": "numeric", "data_type": "numeric"}],
        "exposures": {
            "ZOMG": {
                "meta": {
                    "referenced_columns": [
                        {"column_name": "order_id", "data_type": "string"}
                    ]
                },
                "url": "http://bla.com",
                "name": "ZOMG",
                "depends_on": {"nodes": ["models.exposures_test"]},
            }
        },
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, columns=[dict(name="bla")], as_model=True
    )

    assert "order_id column missing in the model" in test_result["test_results_query"]
    assert test_result["status"] == "fail"
