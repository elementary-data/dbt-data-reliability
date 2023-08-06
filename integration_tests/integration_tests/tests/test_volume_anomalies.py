from datetime import date, datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}


def test_anomalyless_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=date.today())
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "pass"


def test_full_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=date.today())
        if cur_date < cur_date.today() - timedelta(days=1)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_partial_drop_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=date.today())
        for _ in range(2 if cur_date < cur_date.today() - timedelta(days=1) else 1)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


def test_spike_table_volume_anomalies(test_id: str, dbt_project: DbtProject):
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=date.today())
        for _ in range(1 if cur_date < cur_date.today() - timedelta(days=1) else 2)
    ]
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, DBT_TEST_ARGS)
    assert test_result["status"] == "fail"


@Parametrization.autodetect_parameters()
@Parametrization.case(name="source", as_model=False)
@Parametrization.case(name="model", as_model=True)
def test_volume_anomalies_with_where_parameter(
    test_id: str, dbt_project: DbtProject, as_model: bool
):
    test_date, *training_dates = generate_dates(base_date=date.today() - timedelta(1))

    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "payback": payback}
        for payback in ["karate", "ka-razy", "ka-razy", "ka-razy", "ka-razy", "ka-razy"]
    ]
    data += [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "payback": payback}
        for cur_date in training_dates
        for payback in ["karate", "ka-razy"]
    ]

    params = DBT_TEST_ARGS
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, as_model=as_model
    )
    assert test_result["status"] == "fail"

    params = dict(DBT_TEST_ARGS, where="payback = 'karate'")
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, as_model=as_model
    )
    assert test_result["status"] == "pass"

    params = dict(DBT_TEST_ARGS, where="payback = 'ka-razy'")
    test_result = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params, as_model=as_model
    )
    assert test_result["status"] == "fail"


def test_volume_anomalies_with_time_buckets(test_id: str, dbt_project: DbtProject):
    now = datetime.utcnow()
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in generate_dates(base_date=now, period="hours", days_back=2)
        if cur_date < now - timedelta(hours=1)
    ]
    test_args = {
        **DBT_TEST_ARGS,
        "time_bucket": {"period": "hour", "count": 1},
        "days_back": 1,
    }
    test_result = dbt_project.test(data, test_id, DBT_TEST_NAME, test_args)
    assert test_result["status"] == "fail"
