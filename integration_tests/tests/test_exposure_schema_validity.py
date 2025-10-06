import pytest
from dbt_project import DbtProject

DBT_TEST_NAME = "elementary.exposure_schema_validity"


def seed(dbt_project: DbtProject):
    seed_result = dbt_project.dbt_runner.seed(full_refresh=True)
    assert seed_result is True


@pytest.mark.skip_for_dbt_fusion
def test_exposure_schema_validity_existing_exposure_yml_invalid(
    test_id: str, dbt_project: DbtProject
):
    seed(dbt_project)
    run_result = dbt_project.dbt_runner.run(
        select="orders", full_refresh=True, quiet=True
    )
    assert run_result is True
    test_result = dbt_project.dbt_runner._run_command(
        command_args=["test", "-s", "tag:exposure_orders"],
        log_format="text",
        capture_output=True,
        quiet=True,
        log_output=False,
    )
    assert test_result.success is False


@pytest.mark.skip_for_dbt_fusion
def test_exposure_schema_validity_existing_exposure_yml_valid(
    test_id: str, dbt_project: DbtProject
):
    seed(dbt_project)
    run_result = dbt_project.dbt_runner.run(
        select="customers", full_refresh=True, quiet=True
    )
    assert run_result is True
    test_result = dbt_project.dbt_runner._run_command(
        command_args=["test", "-s", "tag:exposure_customers"],
        capture_output=True,
        quiet=True,
        log_output=False,
    )
    assert test_result.success is True


@pytest.mark.skip_targets(["spark"])
@pytest.mark.skip_for_dbt_fusion
def test_exposure_schema_validity_no_exposures(test_id: str, dbt_project: DbtProject):
    test_result = dbt_project.test(test_id, DBT_TEST_NAME)
    assert test_result["status"] == "pass"


# Schema validity currently not supported on ClickHouse
@pytest.mark.skip_targets(["spark", "clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_exposure_schema_validity_correct_columns_and_types(
    test_id: str, dbt_project: DbtProject
):
    target_data_type = (
        "other" if dbt_project.dbt_runner.target == "dremio" else "string"
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
                            "data_type": target_data_type,
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
@pytest.mark.skip_for_dbt_fusion
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


# Schema validity currently not supported on ClickHouse
@pytest.mark.skip_targets(["spark", "clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_exposure_schema_validity_invalid_type_name_present_in_error(
    test_id: str, dbt_project: DbtProject
):
    # Specify valid type per target
    data_type = {
        "snowflake": "NUMERIC",
        "bigquery": "NUMERIC",
        "spark": "int",
        "databricks": "int",
        "databricks_catalog": "int",
        "athena": "int",
        "trino": "int",
        "dremio": "int",
    }.get(dbt_project.dbt_runner.target, "numeric")
    DBT_TEST_ARGS = {
        "node": "models.exposures_test",
        "columns": [{"name": "order_id", "dtype": data_type, "data_type": data_type}],
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
        "different data type for the column order_id string vs numeric"
        in test_result["test_results_query"]
    )
    assert test_result["status"] == "fail"


@pytest.mark.skip_targets(["spark"])
@pytest.mark.skip_for_dbt_fusion
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
@pytest.mark.skip_for_dbt_fusion
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
