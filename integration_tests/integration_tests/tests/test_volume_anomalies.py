from datetime import date, timedelta

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

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


def test_volume_anomalies_with_where_parameter(test_id: str, dbt_project: DbtProject):
    dates = generate_dates(base_date=date.today() - timedelta(1))
    data = [
        # date[0] is yesterday
        {TIMESTAMP_COLUMN: dates[0], "payback": "karate"},
        {TIMESTAMP_COLUMN: dates[0], "payback": "ka-razy"},
        {TIMESTAMP_COLUMN: dates[0], "payback": "ka-razy"},
        {TIMESTAMP_COLUMN: dates[0], "payback": "ka-razy"},
        {TIMESTAMP_COLUMN: dates[0], "payback": "ka-razy"},
        {TIMESTAMP_COLUMN: dates[0], "payback": "ka-razy"},
    ] + sum(
        [
            [
                {TIMESTAMP_COLUMN: cur_date, "payback": "karate"},
                {TIMESTAMP_COLUMN: cur_date, "payback": "ka-razy"},
            ]
            for cur_date in dates[1:]
        ],
        [],
    )

    params_without_where = DBT_TEST_ARGS
    result_without_where = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_without_where
    )
    assert result_without_where["status"] == "fail"

    params_with_where = dict(params_without_where, where="payback = 'karate'")
    result_with_where = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_with_where
    )
    assert result_with_where["status"] == "pass"

    params_with_where2 = dict(params_without_where, where="payback = 'ka-razy'")
    result_with_where2 = dbt_project.test(
        data, test_id, DBT_TEST_NAME, params_with_where2
    )
    assert result_with_where2["status"] == "fail"
