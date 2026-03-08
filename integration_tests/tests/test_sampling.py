import json

from dbt_project import DbtProject

COLUMN_NAME = "some_column"


TEST_SAMPLE_ROW_COUNT = 7


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
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    assert all([row == {COLUMN_NAME: None} for row in samples])
