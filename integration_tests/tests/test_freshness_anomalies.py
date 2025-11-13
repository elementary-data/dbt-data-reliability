from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import chain

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject
from parametrization import Parametrization

TEST_NAME = "elementary.freshness_anomalies"
TIMESTAMP_COLUMN = "timestamp"


@dataclass
class FreshnessAnomaliesConfig:
    period: str
    step: timedelta
    days_back: int
    backfill_days: int
    detection_delay_hours: int


HOURLY_CONFIG = FreshnessAnomaliesConfig(
    period="hour",
    step=timedelta(minutes=10),
    days_back=14,
    backfill_days=2,
    detection_delay_hours=0,
)

DAILY_CONFIG = FreshnessAnomaliesConfig(
    period="day",
    step=timedelta(hours=2),
    days_back=30,
    backfill_days=3,
    detection_delay_hours=0,
)

WEEKLY_CONFIG = FreshnessAnomaliesConfig(
    period="week",
    step=timedelta(hours=12),
    days_back=7 * 15,
    backfill_days=14,
    detection_delay_hours=0,
)

MONTHLY_CONFIG = FreshnessAnomaliesConfig(
    period="month",
    step=timedelta(days=2),
    days_back=30 * 15,
    backfill_days=60,
    detection_delay_hours=0,
)


@Parametrization.autodetect_parameters()
@Parametrization.case(name="hourly", config=HOURLY_CONFIG)
@Parametrization.case(name="daily", config=DAILY_CONFIG)
@Parametrization.case(name="weekly", config=WEEKLY_CONFIG)
@Parametrization.case(name="monthly", config=MONTHLY_CONFIG)
class TestFreshnessAnomalies:
    def _get_test_config(self, config: FreshnessAnomaliesConfig) -> dict:
        return dict(
            timestamp_column=TIMESTAMP_COLUMN,
            days_back=config.days_back,
            backfill_days=config.backfill_days,
            time_bucket=dict(period=config.period, count=1),
            detection_delay=dict(period="hour", count=config.detection_delay_hours),
        )

    def _skip_redshift_monthly(
        self, target: str, config: FreshnessAnomaliesConfig
    ) -> bool:
        if target == "redshift" and config.period == "month":
            pytest.skip("Redshift does not support monthly time buckets.")

    # Anomalies currently not supported on ClickHouse
    @pytest.mark.skip_targets(["clickhouse"])
    def test_anomalyless_table(
        self,
        test_id: str,
        dbt_project: DbtProject,
        config: FreshnessAnomaliesConfig,
        target: str,
    ):
        self._skip_redshift_monthly(target, config)
        data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                datetime.now(), step=config.step, days_back=config.days_back
            )
        ]
        result = dbt_project.test(
            test_id, TEST_NAME, self._get_test_config(config), data=data
        )
        assert result["status"] == "pass"

    # Anomalies currently not supported on ClickHouse
    @pytest.mark.skip_targets(["clickhouse"])
    def test_stop(
        self,
        test_id: str,
        dbt_project: DbtProject,
        config: FreshnessAnomaliesConfig,
        target: str,
    ):
        self._skip_redshift_monthly(target, config)
        anomaly_date = datetime.now() - timedelta(days=config.backfill_days)
        data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                anomaly_date, step=config.step, days_back=config.days_back
            )
        ]
        result = dbt_project.test(
            test_id, TEST_NAME, self._get_test_config(config), data=data
        )
        assert result["status"] == "fail"

    # Anomalies currently not supported on ClickHouse
    @pytest.mark.skip_targets(["clickhouse"])
    def test_stop_with_delay(
        self,
        test_id: str,
        dbt_project: DbtProject,
        config: FreshnessAnomaliesConfig,
        target: str,
    ):
        self._skip_redshift_monthly(target, config)
        anomaly_date = datetime.now() - timedelta(days=config.backfill_days)
        data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                anomaly_date, step=config.step, days_back=(config.days_back)
            )
        ]
        delayed_config = copy(config)
        delayed_config.detection_delay_hours = 24 * config.backfill_days
        result = dbt_project.test(
            test_id, TEST_NAME, self._get_test_config(delayed_config), data=data
        )
        assert result["status"] == "pass"

    # Anomalies currently not supported on ClickHouse
    @pytest.mark.skip_targets(["clickhouse"])
    def test_slower_rate(
        self,
        test_id: str,
        dbt_project: DbtProject,
        config: FreshnessAnomaliesConfig,
        target: str,
    ):
        self._skip_redshift_monthly(target, config)
        anomaly_date = datetime.now() - timedelta(days=config.backfill_days)
        data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                anomaly_date, step=config.step, days_back=config.days_back
            )
        ]
        slow_data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                datetime.now(), step=config.step * 4, days_back=config.backfill_days
            )
        ]
        data.extend(slow_data)
        result = dbt_project.test(
            test_id, TEST_NAME, self._get_test_config(config), data=data
        )
        assert result["status"] == "fail"

    # Anomalies currently not supported on ClickHouse
    @pytest.mark.skip_targets(["clickhouse"])
    def test_faster_rate(
        self,
        test_id: str,
        dbt_project: DbtProject,
        config: FreshnessAnomaliesConfig,
        target: str,
    ):
        self._skip_redshift_monthly(target, config)
        anomaly_date = datetime.now() - timedelta(days=config.backfill_days)
        data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                anomaly_date, step=config.step, days_back=config.days_back
            )
        ]
        fast_data = [
            {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
            for date in generate_dates(
                datetime.now(), step=config.step / 4, days_back=config.backfill_days
            )
        ]
        data.extend(fast_data)
        result = dbt_project.test(
            test_id, TEST_NAME, self._get_test_config(config), data=data
        )
        assert result["status"] == "pass"


# Anomalies currently not supported on ClickHouse
@pytest.mark.skip_targets(["clickhouse"])
def test_first_metric_null(test_id, dbt_project: DbtProject):
    config = dict(
        timestamp_column=TIMESTAMP_COLUMN,
        days_back=3,
        backfill_days=2,
        time_bucket=dict(period="day", count=1),
        sensitivity=1,
    )
    new_data = list(
        chain.from_iterable(
            [
                [
                    {TIMESTAMP_COLUMN: datetime(2000, 1, d, h, 0).strftime(DATE_FORMAT)}
                    for h in range(8, 23)
                ]
                for d in range(1, 6)
            ]
        )
    )
    for i in [3, 4]:
        result = dbt_project.test(
            test_id,
            TEST_NAME,
            config,
            data=new_data,
            test_vars={"custom_run_started_at": datetime(2000, 1, i).isoformat()},
            as_model=True,
            materialization="incremental",
        )
        assert result["status"] == "pass"


@pytest.mark.skip_targets(["clickhouse"])
def test_exclude_detection_from_training(test_id: str, dbt_project: DbtProject):
    """
    Test exclude_detection_period_from_training flag for freshness anomalies.

    Data: 7 days normal (frequent updates, days -14 to -8) + 7 days anomalous (1 update/day, days -7 to -1)
    Without exclusion: anomalous data in training baseline → test passes
    With exclusion: anomalous data excluded from training → test fails
    """
    utc_now = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)

    # Generate 7 days of normal data with frequent updates (every 2 hours) from day -14 to day -8
    normal_data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(
            base_date=utc_now - timedelta(days=8),
            step=timedelta(hours=2),
            days_back=7,
        )
    ]

    # Generate 7 days of anomalous data (only 1 update per day at noon) from day -7 to day -1
    anomalous_data = [
        {TIMESTAMP_COLUMN: date.strftime(DATE_FORMAT)}
        for date in generate_dates(
            base_date=(utc_now - timedelta(days=1)).replace(hour=12, minute=0),
            step=timedelta(hours=24),
            days_back=7,
        )
    ]

    all_data = normal_data + anomalous_data

    # Test 1: WITHOUT exclusion (should pass - training includes detection window with anomalous pattern)
    test_args_without_exclusion = {
        "timestamp_column": TIMESTAMP_COLUMN,
        "training_period": {"period": "day", "count": 14},
        "detection_period": {"period": "day", "count": 7},
        "time_bucket": {"period": "day", "count": 1},
        "days_back": 20,
        "backfill_days": 0,
        "sensitivity": 3,
        "min_training_set_size": 3,
        "anomaly_direction": "spike",
        "ignore_small_changes": {
            "spike_failure_percent_threshold": 0,
            "drop_failure_percent_threshold": 0,
        },
    }

    detection_end = utc_now

    test_result_without_exclusion = dbt_project.test(
        test_id + "_without_exclusion",
        TEST_NAME,
        test_args_without_exclusion,
        data=all_data,
        test_vars={"custom_run_started_at": detection_end.isoformat()},
    )

    # This should PASS because the anomaly is included in training, making it part of the baseline
    assert (
        test_result_without_exclusion["status"] == "pass"
    ), "Test should pass when anomaly is included in training"

    # Test 2: WITH exclusion (should fail - detects the anomaly because it's excluded from training)
    test_args_with_exclusion = {
        **test_args_without_exclusion,
        "exclude_detection_period_from_training": True,
    }

    test_result_with_exclusion = dbt_project.test(
        test_id + "_with_exclusion",
        TEST_NAME,
        test_args_with_exclusion,
        data=all_data,
        test_vars={"custom_run_started_at": detection_end.isoformat()},
    )

    # This should FAIL because the anomaly is excluded from training, so it's detected as anomalous
    assert (
        test_result_with_exclusion["status"] == "fail"
    ), "Test should fail when anomaly is excluded from training"
