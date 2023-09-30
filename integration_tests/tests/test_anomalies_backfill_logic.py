from datetime import datetime, timedelta

import dateutil.parser
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

BACKFILL_DAYS = 2
DAYS_BACK = 14
TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}

LATEST_METRICS_QUERY = """
    with metrics_ordered as (
        select
            bucket_start,
            metric_value,
            row_number() over (partition by id order by updated_at desc) as row_number
        from {{{{ ref("data_monitoring_metrics") }}}}
        where metric_name = 'row_count' and lower(full_table_name) like '%{test_id}'
    )
    select bucket_start, metric_value from metrics_ordered
    where row_number = 1
"""


def get_row_count_metrics(dbt_project: DbtProject, test_id: str):
    results = dbt_project.run_query(LATEST_METRICS_QUERY.format(test_id=test_id))
    return {
        dateutil.parser.parse(result["bucket_start"]).date(): int(
            result["metric_value"]
        )
        for result in results
    }


def test_full_backfill_for_non_incremental_model(dbt_project: DbtProject, test_id: str):
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, as_model=True
    )
    assert test_result["status"] == "pass"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]
    test_result = dbt_project.test(
        test_id, DBT_TEST_NAME, DBT_TEST_ARGS, data=data, as_model=True
    )
    assert test_result["status"] == "pass"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }


def test_partial_backfill_for_incremental_models(dbt_project: DbtProject, test_id: str):
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
    )
    assert test_result["status"] == "pass"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }

    # Reload data to the table with 1 row per date instead of 5. If the backfill logic is working,
    # only metrics for the last 2 days should be updated and the test should fail because the metric
    # drops from 5 to 1 in these days.
    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
    )
    assert test_result["status"] == "fail"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(BACKFILL_DAYS) else 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }


def test_longer_backfill_in_case_of_a_gap(dbt_project: DbtProject, test_id: str):
    date_gap_size = 5
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
        if cur_date < utc_today - timedelta(date_gap_size)
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={
            "custom_run_started_at": (
                datetime.utcnow() - timedelta(date_gap_size)
            ).isoformat()
        },
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if utc_today - timedelta(DAYS_BACK + date_gap_size)
        <= cur_date
        < utc_today - timedelta(date_gap_size)
    }

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(date_gap_size) else 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK + date_gap_size)
    }


def test_full_backfill_if_metric_not_updated_for_a_long_time(
    dbt_project: DbtProject, test_id: str
):
    date_gap_size = 15
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
        if cur_date < utc_today - timedelta(date_gap_size)
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={
            "custom_run_started_at": (
                datetime.utcnow() - timedelta(date_gap_size)
            ).isoformat()
        },
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if utc_today - timedelta(DAYS_BACK + date_gap_size)
        <= cur_date
        < utc_today - timedelta(date_gap_size)
    }

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(DAYS_BACK) else 1
        for cur_date in data_dates
        if (
            utc_today - timedelta(DAYS_BACK + date_gap_size)
            <= cur_date
            < utc_today - timedelta(date_gap_size)
            or cur_date >= utc_today - timedelta(DAYS_BACK)
        )
    }


def test_backfill_when_metric_doesnt_exist_back_enough(
    dbt_project: DbtProject, test_id: str
):
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={"days_back": 21},
    )
    assert test_result["status"] != "error"
    assert get_row_count_metrics(dbt_project, test_id) == {
        cur_date: 1 for cur_date in data_dates if cur_date >= utc_today - timedelta(21)
    }
