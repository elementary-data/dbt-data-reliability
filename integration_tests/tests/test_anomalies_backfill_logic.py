import json
from datetime import datetime, time, timedelta

import dateutil.parser
import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

BACKFILL_DAYS = 2
DAYS_BACK = 14
TIMESTAMP_COLUMN = "updated_at"
DBT_TEST_NAME = "elementary.volume_anomalies"
DBT_TEST_ARGS = {"timestamp_column": TIMESTAMP_COLUMN}

# This returns the latest metrics in the DB per bucket (but not necessarily all from the same run)
LATEST_METRICS_QUERY = """
    with metrics_ordered as (
        select
            bucket_start,
            bucket_end,
            metric_value,
            row_number() over (partition by id order by updated_at desc) as row_num
        from {{{{ ref("data_monitoring_metrics") }}}}
        where metric_name = 'row_count' and lower(full_table_name) like '%{test_id}'
    )
    select bucket_start, bucket_end, metric_value from metrics_ordered
    where row_num = 1
"""

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


def get_row_count_metrics(dbt_project: DbtProject, test_id: str):
    results = dbt_project.run_query(LATEST_METRICS_QUERY.format(test_id=test_id))
    return {
        (
            dateutil.parser.parse(result["bucket_start"]).replace(tzinfo=None),
            dateutil.parser.parse(result["bucket_end"]).replace(tzinfo=None),
        ): result["metric_value"]
        for result in results
    }


def get_daily_row_count_metrics(dbt_project: DbtProject, test_id: str):
    row_count_metrics = get_row_count_metrics(dbt_project, test_id)
    return {
        bucket_start.date(): metric_value
        for (bucket_start, _), metric_value in row_count_metrics.items()
    }


def get_latest_anomaly_test_metrics(dbt_project: DbtProject, test_id: str):
    results = dbt_project.run_query(ANOMALY_TEST_POINTS_QUERY.format(test_id=test_id))
    result_rows = [json.loads(result["result_row"]) for result in results]
    return {
        (
            dateutil.parser.parse(result["bucket_start"]).replace(tzinfo=None),
            dateutil.parser.parse(result["bucket_end"]).replace(tzinfo=None),
        ): result["metric_value"]
        for result in result_rows
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(BACKFILL_DAYS) else 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK)
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(date_gap_size) else 1
        for cur_date in data_dates
        if cur_date >= utc_today - timedelta(DAYS_BACK + date_gap_size)
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < utc_today - timedelta(DAYS_BACK) else 1
        for cur_date in data_dates
        if (
            utc_today - timedelta(DAYS_BACK + date_gap_size)
            <= cur_date
            < utc_today - timedelta(date_gap_size)
            or cur_date >= utc_today - timedelta(DAYS_BACK)
        )
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 1 for cur_date in data_dates if cur_date >= utc_today - timedelta(21)
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_backfill_with_middle_buckets_gap(dbt_project: DbtProject, test_id: str):
    utc_today = datetime.utcnow().date()
    data_start = utc_today - timedelta(21)
    date_gap_start = utc_today - timedelta(14)
    date_gap_end = utc_today - timedelta(7)
    data_dates = generate_dates(base_date=utc_today - timedelta(1))

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)}
        for cur_date in data_dates
        for _ in range(5)
    ]

    # Here we simulate a historic run of 7 days
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={
            "custom_run_started_at": date_gap_start.isoformat(),
            "days_back": (date_gap_start - data_start).days,
        },
    )
    assert test_result["status"] != "error"

    # And here a more recent one of 7 days (which is shorter than the accumulated gap)
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={"days_back": (utc_today - date_gap_end).days - 1},
    )
    assert test_result["status"] != "error"

    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5
        for cur_date in data_dates
        if (data_start <= cur_date < date_gap_start)
        or (date_gap_end < cur_date <= utc_today)
    }

    # Now we increase the days_back - and we expect the backfill to account for the missing days in the middle
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
    assert get_daily_row_count_metrics(dbt_project, test_id) == {
        cur_date: 5 if cur_date < date_gap_start else 1
        for cur_date in data_dates
        if cur_date >= data_start
    }


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_bucket_size_not_aligned_with_days(dbt_project: DbtProject, test_id: str):
    """
    In this test we choose a bucket size that is not aligned with one day - specifically 7 hours.
    In such a scenario, we'll always need a full backfill since buckets computed yesterday will be different than
    ones computed today, e.g.:
    - 01-01 00:00:00, 01-01 07:00:00, 01-01 14:00:00, 01-01 21:00:00, 02-01 04:00:00, 02-01 11:00:00 ...
    vs
    - 02-01 00:00:00, 02-01 07:00:00, 02-01 14:00:00, 02-01 21:00:00, 03-01 04:00:00, 03-01 11:00:00 ...

    We also want to see that the "stale" buckets are not included in the computation
    """
    utc_today = datetime.utcnow().date()
    data_dates = generate_dates(
        base_date=utc_today, step=timedelta(hours=1), days_back=4
    )

    data = [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT)} for cur_date in data_dates
    ]

    # Here we simulate a previous day's run
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={
            "custom_run_started_at": (utc_today - timedelta(1)).isoformat(),
            "time_bucket": {"period": "hour", "count": 7},
            "days_back": 2,
        },
    )
    assert test_result["status"] == "pass"

    row_count_metrics = get_row_count_metrics(dbt_project, test_id)
    start_bucket = datetime.combine(utc_today - timedelta(3), time.min)
    expected_metrics = {
        (
            start_bucket + timedelta(hours=7 * i),
            start_bucket + timedelta(hours=7 * (i + 1)),
        ): 7
        for i in range(6)
    }
    assert row_count_metrics == expected_metrics

    # We expect the metrics in the test results to be all the metrics except the first, since we
    # currently exclude it (as it doesn't have a score / range based on previous metrics)
    anomaly_test_metrics = get_latest_anomaly_test_metrics(dbt_project, test_id)
    expected_test_metrics = {
        k: v for k, v in expected_metrics.items() if k[0] != start_bucket
    }
    assert anomaly_test_metrics == expected_test_metrics

    # Here we simulate a previous day's run
    test_result = dbt_project.test(
        test_id,
        DBT_TEST_NAME,
        DBT_TEST_ARGS,
        data=data,
        as_model=True,
        materialization="incremental",
        test_vars={
            "custom_run_started_at": utc_today.isoformat(),
            "time_bucket": {"period": "hour", "count": 7},
            "days_back": 2,
        },
    )
    assert test_result["status"] != "error"

    row_count_metrics = get_row_count_metrics(dbt_project, test_id)
    assert (
        len(row_count_metrics) == 12
    )  # No overlap between previous day and today, since the bucket size
    # is not divisible by 24 (and prime)

    anomaly_test_metrics = get_latest_anomaly_test_metrics(dbt_project, test_id)
    start_bucket = datetime.combine(utc_today - timedelta(2), time.min)
    expected_test_metrics = {
        (
            start_bucket + timedelta(hours=7 * i),
            start_bucket + timedelta(hours=7 * (i + 1)),
        ): 7
        for i in range(1, 6)
    }
    assert anomaly_test_metrics == expected_test_metrics
