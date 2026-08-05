import json

from dbt_project import DbtProject

COLUMN_NAME = "some_column"
CONTEXT_COLUMN = "context_column"

TEST_SAMPLE_ROW_COUNT = 7


def test_with_context_test_is_sampled(test_id: str, dbt_project: DbtProject):
    """with_context tests rely on the test materialization to capture their failing rows."""
    null_count = 50
    data = [
        {COLUMN_NAME: None, CONTEXT_COLUMN: f"context-{index}"}
        for index in range(null_count)
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.not_null_with_context",
        dict(column_name=COLUMN_NAME, context_columns=[CONTEXT_COLUMN]),
        data=data,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
        },
    )
    assert test_result["status"] == "fail"
    assert test_result["failed_row_count"] == null_count
    # Must stay dbt_test: the alerts models partition on test_type exactly.
    assert test_result["test_type"] == "dbt_test"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    # Membership, since sampling returns an arbitrary TEST_SAMPLE_ROW_COUNT of the failing rows.
    expected_context_values = {f"context-{index}" for index in range(null_count)}
    for sample in samples:
        assert sample[COLUMN_NAME] is None
        assert sample[CONTEXT_COLUMN] in expected_context_values
