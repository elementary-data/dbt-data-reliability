import json

import pytest
from dbt_project import DbtProject

SENSITIVE_COLUMN = "email"
SAFE_COLUMN = "order_count"

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

TEST_SAMPLE_ROW_COUNT = 5


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_enabled(test_id: str, dbt_project: DbtProject):
    """Test that PII columns are excluded when column-level PII protection is enabled"""
    data = [
        {SENSITIVE_COLUMN: f"user{i}@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        test_args=dict(column_name=SAFE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_columns": True,
            "pii_column_tags": ["pii"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    for sample in samples:
        assert SENSITIVE_COLUMN not in sample
        assert SAFE_COLUMN in sample


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_disabled(test_id: str, dbt_project: DbtProject):
    """Test that all columns are included when column-level PII protection is disabled"""
    data = [
        {SENSITIVE_COLUMN: f"user{i}@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        test_args=dict(column_name=SAFE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_columns": False,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    for sample in samples:
        assert SENSITIVE_COLUMN in sample
        assert SAFE_COLUMN in sample


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_tags_exist_but_flag_disabled(
    test_id: str, dbt_project: DbtProject
):
    """Test that when PII tags exist but disable_samples_on_pii_columns is false, samples are collected normally"""
    data = [
        {SENSITIVE_COLUMN: f"user{i}@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        test_args=dict(column_name=SAFE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_columns": False,  # Flag is disabled
            "pii_column_tags": ["pii"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    for sample in samples:
        assert (
            SENSITIVE_COLUMN in sample
        )  # PII column should be included when flag is disabled
        assert SAFE_COLUMN in sample


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_all_columns_pii(test_id: str, dbt_project: DbtProject):
    """Test behavior when all columns are tagged as PII"""
    data = [
        {SENSITIVE_COLUMN: f"user{i}@example.com", SAFE_COLUMN: i} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        test_args=dict(column_name=SAFE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN, "config": {"tags": ["pii"]}},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_columns": True,
            "pii_column_tags": ["pii"],
        },
    )
    assert test_result["status"] == "pass"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == TEST_SAMPLE_ROW_COUNT
    for sample in samples:
        assert "_no_non_excluded_columns" in sample
        assert sample["_no_non_excluded_columns"] == 1
        assert SENSITIVE_COLUMN not in sample
        assert SAFE_COLUMN not in sample
