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
        {SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_disabled(test_id: str, dbt_project: DbtProject):
    """Test that all columns are included when column-level PII protection is disabled"""
    data = [
        {SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": False,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    # sample should be {'unique_field': 'user@example.com', 'n_records': 10}
    assert len(samples) == 1
    assert "unique_field" in samples[0]
    assert samples[0]["unique_field"] == "user@example.com"
    assert "n_records" in samples[0]
    assert samples[0]["n_records"] == 10


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_default_tag_override(test_id: str, dbt_project: DbtProject):
    """Test that default PII tag can be overridden with a custom tag"""
    data = [
        {SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: None} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["sensitive"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    # sample should be {'unique_field': 'user@example.com', 'n_records': 10}
    assert len(samples) == 1
    assert "unique_field" in samples[0]
    assert samples[0]["unique_field"] == "user@example.com"
    assert "n_records" in samples[0]
    assert samples[0]["n_records"] == 10


@pytest.mark.skip_targets(["clickhouse"])
def test_column_pii_sampling_tags_exist_but_flag_disabled(
    test_id: str, dbt_project: DbtProject
):
    """Test that when PII tags exist but disable_samples_on_pii_tags is false, samples are collected normally"""
    data = [{SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: 1} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SAFE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pIi"]}},
            {"name": SAFE_COLUMN},
        ],
        test_column=None,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": False,  # Flag is disabled
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    # When flag is disabled, we get the full sample (not limited by PII filtering)
    assert len(samples) == 1
    assert "unique_field" in samples[0]
    assert samples[0]["unique_field"] == 1
    assert "n_records" in samples[0]
    assert samples[0]["n_records"] == 10


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
        test_column=None,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii"],
        },
    )
    assert test_result["status"] == "pass"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    # When all columns are PII, no samples should be collected
    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_unique_test_custom_tag(test_id: str, dbt_project: DbtProject):
    """Test that column mapping correctly maps unique test columns"""
    data = [{SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: i} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["custom_tag"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["custom_tag"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_accepted_values_multi_tags(test_id: str, dbt_project: DbtProject):
    """Test that column mapping correctly maps accepted_values test columns"""
    data = [{SENSITIVE_COLUMN: "invalid_value", SAFE_COLUMN: i} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "accepted_values",
        test_args=dict(column_name=SENSITIVE_COLUMN, values=["valid1", "valid2"]),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii", "custom_tag"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_not_null_test_multi_matched_tags(test_id: str, dbt_project: DbtProject):
    """Test that column mapping correctly handles not_null test columns"""
    data = [{SENSITIVE_COLUMN: None, SAFE_COLUMN: i} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii", "sensitive"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii", "sensitive"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_multiple_pii_columns_mapping(test_id: str, dbt_project: DbtProject):
    """Test that column mapping handles multiple PII columns correctly"""
    data = [
        {SENSITIVE_COLUMN: "user@example.com", "phone": "123-456-7890", SAFE_COLUMN: i}
        for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": "phone", "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_custom_sql_test_with_pii_column_simple(test_id: str, dbt_project: DbtProject):
    """Test that custom SQL tests with PII columns are handled correctly"""
    data = [{SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: i} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "unique",
        test_args=dict(column_name=SENSITIVE_COLUMN),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"tags": ["pii"]}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_meta_tags_and_accepted_values(test_id: str, dbt_project: DbtProject):
    data = [{SENSITIVE_COLUMN: "user@example.com", SAFE_COLUMN: i} for i in range(10)]

    # Test with accepted_values to simulate complex column mapping
    test_result = dbt_project.test(
        test_id,
        "accepted_values",
        test_args=dict(column_name=SENSITIVE_COLUMN, values=["invalid@example.com"]),
        data=data,
        columns=[
            {"name": SENSITIVE_COLUMN, "config": {"meta": {"tags": ["pii"]}}},
            {"name": SAFE_COLUMN},
        ],
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0
