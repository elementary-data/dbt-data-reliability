import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "sensitive_data"

SAMPLES_QUERY = """
    with latest_elementary_test_result as (
        select id
        from {{ ref("elementary_test_results") }}
        where lower(table_name) = lower('{test_id}')
        order by created_at desc
        limit 1
    )

    select result_row
    from {{ ref("test_result_rows") }}
    where elementary_test_results_id in (select * from latest_elementary_test_result)
"""


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_config_prevents_sampling(
    test_id: str, dbt_project: DbtProject
):
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    columns = [
        {
            "name": COLUMN_NAME,
            "config": {"disable_samples": True},
            "tests": [{"not_null": {}}],
        }
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        columns=columns,
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_false_allows_sampling(test_id: str, dbt_project: DbtProject):
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    columns = [
        {
            "name": COLUMN_NAME,
            "config": {"disable_samples": False},
            "tests": [{"not_null": {}}],
        }
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        columns=columns,
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 5
    assert all([row == {COLUMN_NAME: None} for row in samples])


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_config_overrides_pii_tags(
    test_id: str, dbt_project: DbtProject
):
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    columns = [
        {
            "name": COLUMN_NAME,
            "config": {"disable_samples": True, "tags": ["pii"]},
            "tests": [{"not_null": {}}],
        }
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        columns=columns,
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
            "disable_samples_on_pii_columns": True,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 0
