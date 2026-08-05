import json

from dbt_project import DbtProject

COLUMN_NAME = "some_column"
CONTEXT_COLUMN = "context_column"

TEST_SAMPLE_ROW_COUNT = 7


def test_with_context_test_is_sampled(test_id: str, dbt_project: DbtProject):
    """with_context tests must produce result rows.

    They live in the elementary namespace, but unlike the anomaly and schema-change tests they do
    not report their own results -- they are ordinary "return the failing rows" tests, and
    get_test_type classifies them as dbt_test. They therefore rely on the elementary test
    materialization to capture samples. Regression test for the materialization skipping them
    because it branched on the namespace rather than the test type, which left them with no
    result rows at all.
    """
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
    # calculate_failed_count also sits after the early return, so it was skipped too and left
    # failed_row_count null. Asserting it here covers that second path.
    assert test_result["failed_row_count"] == null_count

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(dbt_project.samples_query(test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    # The requested context column travels with the failing row, carrying its real value; that is
    # the whole point of the family. Membership rather than equality because sampling returns an
    # arbitrary TEST_SAMPLE_ROW_COUNT of the null_count failing rows.
    expected_context_values = {f"context-{index}" for index in range(null_count)}
    for sample in samples:
        assert sample[COLUMN_NAME] is None
        assert sample[CONTEXT_COLUMN] in expected_context_values
