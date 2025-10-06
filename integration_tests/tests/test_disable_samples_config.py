import json

import pytest
from dbt_project import DbtProject

COLUMN_NAME = "sensitive_data"

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
def test_disable_samples_config_prevents_sampling(
    test_id: str, dbt_project: DbtProject
):
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
        test_config={"meta": {"disable_test_samples": True}},
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
        test_config={"meta": {"disable_test_samples": False}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 5
    for sample in samples:
        assert COLUMN_NAME in sample
        assert sample[COLUMN_NAME] is None


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_config_overrides_pii_tags(
    test_id: str, dbt_project: DbtProject
):
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
            "disable_samples_on_pii_tags": True,
        },
        test_config={"meta": {"disable_test_samples": True, "tags": ["pii"]}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]
    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_and_pii_interaction(test_id: str, dbt_project: DbtProject):
    """Test that disable_test_samples and PII columns both get excluded"""
    data = [
        {"col1": None, "col2": f"pii{i}", "col3": f"disabled{i}"} for i in range(10)
    ]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name="col1"),
        data=data,
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
            "disable_samples_on_pii_tags": True,
            "pii_tags": ["pii"],
        },
        test_config={"meta": {"disable_test_samples": True}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0


@pytest.mark.skip_targets(["clickhouse"])
def test_disable_samples_with_multiple_columns(test_id: str, dbt_project: DbtProject):
    """Test that disable_test_samples excludes only the disabled column"""
    data = [{"col1": None, "col2": f"value{i}"} for i in range(10)]

    test_result = dbt_project.test(
        test_id,
        "not_null",
        dict(column_name="col1"),
        data=data,
        as_model=True,
        test_vars={
            "enable_elementary_test_materialization": True,
            "test_sample_row_count": 5,
        },
        test_config={"meta": {"disable_test_samples": True}},
    )
    assert test_result["status"] == "fail"

    samples = [
        json.loads(row["result_row"])
        for row in dbt_project.run_query(SAMPLES_QUERY.format(test_id=test_id))
    ]

    assert len(samples) == 0
