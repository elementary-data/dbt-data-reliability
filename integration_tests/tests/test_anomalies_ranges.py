import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.column_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["sum"],
}


ANOMALY_TEST_POINTS_QUERY = """
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


def get_latest_anomaly_test_points(dbt_project: DbtProject, test_id: str):
    results = dbt_project.run_query(ANOMALY_TEST_POINTS_QUERY.format(test_id=test_id))
    return [json.loads(result["result_row"]) for result in results]


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomaly_ranges_are_valid(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "metric": 10}
    ]
    data += [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "metric": 1}
        for cur_date in training_dates
    ]

    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="metric"
    )
    assert test_result["status"] == "fail"

    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)

    # Ensure "is_anomalous" is consistent with whether or not the metric is in the allowed range
    assert all(
        [
            row["is_anomalous"]
            != (row["min_value"] <= row["metric_value"] <= row["max_value"])
            for row in anomaly_test_points
        ]
    )

    # Ensure the range of length zero for all points including the anonmalous point (because for the anomalous point
    # we take the previous point)
    assert all([row["min_value"] == row["max_value"] for row in anomaly_test_points])


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomaly_ranges_are_valid_with_seasonality(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(
        base_date=utc_today - timedelta(1), days_back=7 * 14
    )

    data: List[Dict[str, Any]] = [
        # Make the metric behave like yesterday (instead of like a week ago)
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "metric": ((utc_today - test_date).days + 1) % 7,
        }
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "metric": (utc_today - cur_date).days % 7,
        }
        for cur_date in training_dates
    ]

    test_args = {**DBT_TEST_ARGS, "seasonality": "day_of_week"}
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="metric"
    )

    assert test_result["status"] == "fail"

    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)

    # Ensure "is_anomalous" is consistent with whether the metric is in the allowed range
    assert all(
        [
            row["is_anomalous"]
            != (row["min_value"] <= row["metric_value"] <= row["max_value"])
            for row in anomaly_test_points
        ]
    )

    # Ensure the range 1 -> 1 for all points including the anomalous point (because for the anomalous point
    # we take the previous point's range)
    assert all([row["min_value"] == row["max_value"] for row in anomaly_test_points])
