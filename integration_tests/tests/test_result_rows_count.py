import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_column"

SAMPLES_QUERY = """
    with latest_elementary_test_result as (
        select id
        from {{{{ ref("elementary_test_results") }}}}
        where lower(table_name) = lower('{test_id}')
        order by created_at desc
        limit 1
    )

    select result_row
    from {{{{ ref("test_result_rows") }}}}
    where elementary_test_results_id in (select * from latest_elementary_test_result)
"""


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
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) <= failures


@pytest.mark.skip_targets(["clickhouse"])
def test_sample_percent_adds_description_note(test_id: str, dbt_project: DbtProject):
    """When sample_percent is in test_params, a note should be appended to test_results_description."""
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null_sampled",
        dict(column_name=COLUMN_NAME, sample_percent=10),
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
    )
    assert test_result["status"] == "fail"
    assert "sample_percent" in test_result["test_results_description"]


@pytest.mark.skip_targets(["clickhouse"])
def test_no_sample_percent_no_description_note(
    test_id: str, dbt_project: DbtProject
):
    """When sample_percent is NOT in test_params, no sampling note should appear."""
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
    )
    assert test_result["status"] == "fail"
    description = test_result.get("test_results_description") or ""
    assert "sample_percent" not in description
