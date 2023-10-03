from copy import copy
from dataclasses import dataclass
from datetime import datetime, timedelta

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
