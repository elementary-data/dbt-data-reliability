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

TEST_SAMPLE_ROW_COUNT = 7


# Sampling currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_sampling(test_id: str, dbt_project: DbtProject):
    null_count = 50
    data = [{COLUMN_NAME: None} for _ in range(null_count)]
    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    assert all([row == {COLUMN_NAME: None} for row in samples])
