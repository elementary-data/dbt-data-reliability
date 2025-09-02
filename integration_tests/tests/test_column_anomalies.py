from datetime import datetime, timedelta
from typing import Any, Dict, List

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.column_anomalies"
DBT_TEST_ARGS = {
    "timestamp_column": TIMESTAMP_COLUMN,
    "column_anomalies": ["null_count"],
}


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_column_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="superhero"
    )
    assert test_result["status"] == "pass"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_no_timestamp_column_anomalies(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_args = DBT_TEST_ARGS.copy()
    test_args.pop("timestamp_column")
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )
    assert test_result["status"] == "pass"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalous_column_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "superhero": None}
        for _ in range(3)
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
    ]

    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="superhero"
    )
    assert test_result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_column_anomalies_with_where_parameter(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for universe, superhero in [
            ("DC", None),
            ("DC", None),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ] + [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "universe": universe,
            "superhero": superhero,
        }
        for cur_date in training_dates
        for universe, superhero in [
            ("DC", "Superman"),
            ("DC", "Batman"),
            ("DC", None),
            ("Marvel", "Spiderman"),
        ]
    ]

    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="superhero"
    )
    assert test_result["status"] == "fail"

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        test_column="superhero",
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'Marvel'"},
    )
    assert test_result["status"] == "pass"

    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        test_column="superhero",
        test_vars={"force_metrics_backfill": True},
        test_config={"where": "universe = 'DC'"},
    )
    assert test_result["status"] == "fail"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_column_anomalies_with_timestamp_as_sql_expression(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_args = {
        "timestamp_column": "case when updated_at is not null then updated_at else updated_at end",
        "column_anomalies": ["null_count"],
    }

    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )
    assert test_result["status"] == "pass"


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="true_positive",
    expected_result="fail",
    drop_failure_percent_threshold=5,
    metric_value=10,
)
@Parametrization.case(
    name="false_positive",
    expected_result="fail",
    drop_failure_percent_threshold=None,
    metric_value=1,
)
@Parametrization.case(
    name="true_negative",
    expected_result="pass",
    drop_failure_percent_threshold=5,
    metric_value=1,
)
# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_volume_anomaly_static_data_drop(
    test_id: str,
    dbt_project: DbtProject,
    expected_result: str,
    drop_failure_percent_threshold: int,
    metric_value: int,
):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "superhero": "Batman"}
        for cur_date in generate_dates(base_date=now, step=timedelta(days=1))
        if cur_date < now - timedelta(days=1)
    ] * 50
    data += [
        {
            TIMESTAMP_COLUMN: (now - timedelta(days=1)).strftime(DATE_FORMAT),
            "superhero": "Batman",
        }
    ] * 50
    data += [
        {
            TIMESTAMP_COLUMN: (now - timedelta(days=1)).strftime(DATE_FORMAT),
            "superhero": None,
        }
    ] * metric_value

    # 50 new rows every day with 0 nulls
    # 50 new rows in the last day with 0 nulls
    # <mertic_value> new rows in the last day with nulls

    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "day", "count": 1},
        "column_anomalies": ["not_null_percent"],
        "ignore_small_changes": {
            "drop_failure_percent_threshold": drop_failure_percent_threshold
        },
    }
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )
    assert test_result["status"] == expected_result


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_column_anomalies_group(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
        }
        for cur_date in generate_dates(base_date=utc_today - timedelta(1))
        for superhero in ["Superman", "Batman"]
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, test_column="superhero"
    )
    assert test_result["status"] == "pass"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_column_anomalies_group_by(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
            "dimension": dim,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
        for dim in ["dim1", "dim2"]
    ]

    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": "dim1",
        }
        for _ in range(100)
    ]

    test_args = DBT_TEST_ARGS.copy()
    test_args["dimensions"] = ["dimension"]
    test_args["anomaly_sensitivity"] = 1
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )

    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1

    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": "dim2",
        }
        for _ in range(100)
    ]

    test_args = DBT_TEST_ARGS.copy()
    test_args["dimensions"] = ["dimension"]
    test_args["anomaly_sensitivity"] = 3
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )

    assert test_result["status"] == "fail"
    assert test_result["failures"] == 2


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_column_anomalies_group_by_none_dimension(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
            "dimension": dim,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
        for dim in [None, "dim2"]
    ]

    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": None,
        }
        for _ in range(100)
    ]
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": "dim2",
        }
        for _ in range(100)
    ]

    test_args = DBT_TEST_ARGS.copy()
    test_args["dimensions"] = ["dimension"]
    test_args["anomaly_sensitivity"] = 3
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )

    assert test_result["status"] == "fail"
    assert test_result["failures"] == 2


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_column_anomalies_group_by_multi(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
            "dimension1": dim1,
            "dimension2": dim2,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
        for dim1 in ["dim1", "dim2"]
        for dim2 in ["hey", "bye"]
    ]

    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension1": dim1,
            "dimension2": dim2,
        }
        for _ in range(100)
        for dim1 in ["dim1", "dim2"]
        for dim2 in ["hey"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension1": dim1,
            "dimension2": dim2,
        }
        for _ in range(100)
        for dim1 in ["dim1"]
        for dim2 in ["bye"]
    ]

    test_args = DBT_TEST_ARGS.copy()
    test_args["dimensions"] = ["dimension1", "dimension2"]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )

    assert test_result["status"] == "fail"
    assert test_result["failures"] == 3


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalyless_column_anomalies_group_by_description(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero": superhero,
            "dimension": "super_dimension",
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
    ]
    data += [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero": None,
            "dimension": dim,
        }
        for _ in range(100)
        for dim in ["dim_new", "super_dimension"]
    ]
    test_args = DBT_TEST_ARGS.copy()
    test_args["dimensions"] = ["dimension"]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, test_args, data=data, test_column="superhero"
    )

    assert test_result["status"] == "fail"
    assert test_result["failures"] == 1
    assert "not enough data" not in test_result["test_results_description"].lower()


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_anomalous_boolean_column_anomalies(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    data: List[Dict[str, Any]] = [
        {
            TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT),
            "superhero_has_flown": False,
        }
        for _ in range(3)
    ]
    data += [
        {
            TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT),
            "superhero_has_flown": True,
        }
        for cur_date in training_dates
        for superhero in ["Superman", "Batman"]
    ]

    test_args = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "column_anomalies": ["count_true", "count_false"],
    }
    test_results = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        test_args,
        data=data,
        test_column="superhero_has_flown",
        multiple_results=True,
    )
    assert len(test_results) == 2
    assert {res["status"] for res in test_results} == {"fail"}
    assert {res["test_sub_type"] for res in test_results} == {
        "count_true",
        "count_false",
    }
