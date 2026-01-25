import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "some_data"

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
def test_sample_count_unlimited(test_id: str, dbt_project: DbtProject):
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
        test_config={"meta": {"test_sample_row_count": None}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 20
    for sample in samples:
        assert COLUMN_NAME in sample
        assert sample[COLUMN_NAME] is None


@pytest.mark.skip_targets(["clickhouse"])
def test_sample_count_small(test_id: str, dbt_project: DbtProject):
    null_count = 20
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
        test_config={"meta": {"test_sample_row_count": 3}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 3
    for sample in samples:
        assert COLUMN_NAME in sample
        assert sample[COLUMN_NAME] is None
