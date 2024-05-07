import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.dimension_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": ["superhero"]}

# This returns data points used in the latest anomaly test
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


def test_anomalyless_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "pass"

    # Dimension anomalies only stores anomalous rows (unlike other anomaly tests) - so we should get 0 rows for a passing test.
    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    assert len(anomaly_test_points) == 0


def test_dimension_anomalies_with_timestamp_as_sql_expression(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Spiderman"]
    ]
    test_args = {
        "timestamp_column": "case when updated_at is not null then updated_at else updated_at end",
        "dimensions": ["superhero"],
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "pass"


def test_anomalous_dimension_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for superhero in ["Superman", "Superman", "Superman", "Spiderman"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Spiderman"]
    ]

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"

    # Dimension anomalies only stores anomalous rows (unlike other anomaly tests) - so we should only get 1 row with the problematic value
    anomaly_test_points = get_latest_anomaly_test_points(dbt_project, test_id)
    assert len(anomaly_test_points) == 1
    assert anomaly_test_points[0]["is_anomalous"]
    assert anomaly_test_points[0]["dimension"] == "superhero"
    assert anomaly_test_points[0]["dimension_value"] == "Superman"


def test_dimensions_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for universe, superhero in [
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("DC", "Superman"),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [("DC", "Superman"), ("Marvel", "Spiderman")]
    ]

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, params, data=data)
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="universe = 'Marvel'")
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, params, test_vars={"force_metrics_backfill": True}
    )
    assert test_result["status"] == "pass"

    params = dict(params, where="universe = 'DC'")
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, params, test_vars={"force_metrics_backfill": True}
    )
    assert test_result["status"] == "fail"


def test_dimension_anomalies_with_timestamp_exclude_final_results(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(3))
        for superhero in ["Superman", "Spiderman"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1), days_back=2)
        for superhero in ["Spiderman"]
    ] * 30
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1), days_back=2)
        for superhero in ["Superman"]
    ] * 15

    test_result = dbt_project.test(test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 2

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["superhero"],
        "exclude_final_results": "value > 15",
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "dimensions": ["superhero"],
        "exclude_final_results": "average > 3",
    }
    test_result = dbt_project.test(test_id, DBT_TEST_NAME, test_args, data=data)
    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1
