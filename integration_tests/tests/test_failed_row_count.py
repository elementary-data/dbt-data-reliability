import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_column"


# Failed row count currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_count_failed_row_count(test_id: str, dbt_project: DbtProject):
    null_count = 50
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        test_vars={"enable_elementary_test_materialization": True},
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == null_count
    assert (
        test_result["failed_row_count"] == test_result["failures"]
    )  # when the failed_row_count_calc is count(*), these should be equal


@pytest.mark.skip_for_dbt_fusion
def test_sum_failed_row_count(test_id: str, dbt_project: DbtProject):
    non_unique_count = 50
    data = [{COLUMN_NAME: 5} for _ in range(non_unique_count)]
    test_result = dbt_project.test(
        test_id,
        "unique",
        dict(column_name=COLUMN_NAME),
        data=data,
        test_vars={"enable_elementary_test_materialization": True},
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == non_unique_count
    assert (
        test_result["failed_row_count"] != test_result["failures"]
    )  # when the failed_row_count_calc is sum(<column_name>), these should not be equal


# Failed row count currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_custom_failed_row_count(test_id: str, dbt_project: DbtProject):
    null_count = 50
    overwrite_failed_row_count = 5
    failed_row_count_calc = str(overwrite_failed_row_count)
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(
            column_name=COLUMN_NAME,
            meta=dict(failed_row_count_calc=failed_row_count_calc),
        ),
        data=data,
        test_vars={"enable_elementary_test_materialization": True},
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == overwrite_failed_row_count


@pytest.mark.skip_for_dbt_fusion
def test_warn_if_0(test_id: str, dbt_project: DbtProject):
    # Edge case that we want to verify

    null_count = 50
    data = [{COLUMN_NAME: "pasten"} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME, warn_if="=0"),
        data=data,
        test_vars={"enable_elementary_test_materialization": True},
    )
    assert test_result["status"] == "warn"
    assert test_result["failed_row_count"] == 0
