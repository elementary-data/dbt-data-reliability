import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "value"


SAMPLES_QUERY = """
    with latest_elementary_test_result as (
        select id
        from {{{{ ref("elementary_test_results") }}}}
        where lower(table_name) = lower('{test_id}')
        order by created_at desc, id desc
        limit 1
    )

    select result_row
    from {{{{ ref("test_result_rows") }}}}
    where elementary_test_results_id in (select * from latest_elementary_test_result)
"""

TEST_SAMPLE_ROW_COUNT = 7


@pytest.mark.skip_targets(["clickhouse"])
def test_sampling_pii_disabled(test_id: str, dbt_project: DbtProject):
    """Test that PII-tagged tables don't upload samples even when tests fail"""
    null_count = 50
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        as_model=True,
        model_config={"config": {"tags": ["pii"]}},
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tables": True,
            "pii_table_tags": ["pii", "sensitive"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_sampling_non_pii_enabled(test_id: str, dbt_project: DbtProject):
    """Test that non-PII tables still collect samples normally"""
    null_count = 50
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        as_model=True,
        model_config={"config": {"tags": ["normal"]}},
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tables": True,
            "pii_table_tags": ["pii", "sensitive"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT


@pytest.mark.skip_targets(["clickhouse"])
def test_sampling_pii_feature_disabled(test_id: str, dbt_project: DbtProject):
    """Test that when PII feature is disabled, PII tables still collect samples"""
    null_count = 50
    data = [{COLUMN_NAME: None} for _ in range(null_count)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name=COLUMN_NAME),
        data=data,
        as_model=True,
        model_config={"config": {"tags": ["pii"]}},
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": TEST_SAMPLE_ROW_COUNT,
            "disable_samples_on_pii_tables": False,
            "pii_table_tags": ["pii", "sensitive"],
        },
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == TEST_SAMPLE_ROW_COUNT
