import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_column"


@pytest.mark.skip_targets(["clickhouse"])
def test_result_rows_do_not_exceed_failures(test_id: str, dbt_project: DbtProject):
    """Result rows count should never exceed the dbt failure count."""
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 1000,
        },
    )
    assert test_result["status"] == "fail"

    failures = int(test_result["failures"])
    assert failures == null_count

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]
    assert len(samples) <= failures
