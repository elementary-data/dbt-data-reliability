import json
from dataclasses import dataclass
from typing import Generic, Literal, TypeVar

import pytest
from dbt_project import DbtProject
from parametrization import Parametrization

T = TypeVar("T")


@dataclass
class ParamValues(Generic[T]):
    vars: T
    model: T
    test: T


PARAM_VALUES = {
    "timestamp_column": ParamValues(
        "vars.updated_at", "model.updated_at", "test.updated_at"
    ),
    "where_expression": ParamValues(
        "where = 'var'", "where = 'model'", "where = 'test'"
    ),
    "anomaly_sensitivity": ParamValues(1, 2, 3),
    "anomaly_direction": ParamValues("spike", "drop", "both"),
    "min_training_set_size": ParamValues(10, 20, 30),
    "time_bucket": ParamValues(
        {"count": 1, "period": "day"},
        {"count": 1, "period": "hour"},
        {"count": 1, "period": "day"},
    ),
    "backfill_days": ParamValues(30, 60, 90),
    "seasonality": ParamValues("day_of_week", "hour_of_day", "day_of_week"),
    "event_timestamp_column": ParamValues(
        "vars.updated_at", "model.updated_at", "test.updated_at"
    ),
    "ignore_small_changes": ParamValues(
        {"spike_failure_percent_threshold": 10, "drop_failure_percent_threshold": 10},
        {"spike_failure_percent_threshold": 20, "drop_failure_percent_threshold": 20},
        {"spike_failure_percent_threshold": 30, "drop_failure_percent_threshold": 30},
    ),
    "fail_on_zero": ParamValues(True, False, True),
    "detection_delay": ParamValues(
        {"count": 1, "period": "day"},
        {"count": 2, "period": "day"},
        {"count": 3, "period": "day"},
    ),
    "anomaly_exclude_metrics": ParamValues(
        "where = 'var'", "where = 'model'", "where = 'test'"
    ),
    "detection_period": ParamValues(
        {"count": 1, "period": "day"},
        {"count": 2, "period": "day"},
        {"count": 3, "period": "day"},
    ),
    "training_period": ParamValues(
        {"count": 30, "period": "day"},
        {"count": 60, "period": "day"},
        {"count": 90, "period": "day"},
    ),
    "exclude_final_results": ParamValues(*(["1 = 1"] * 3)),
}


def _get_expected_adapted_config(values_type: Literal["vars", "model", "test"]):
    def get_value(key: str):
        return PARAM_VALUES[key].__dict__[values_type]

    days_back_multiplier = (
        7 if get_value("seasonality") in ["day_of_week", "hour_of_week"] else 1
    )
    return {
        "timestamp_column": get_value("timestamp_column"),
        "where_expression": get_value("where_expression"),
        "anomaly_sensitivity": get_value("anomaly_sensitivity"),
        "anomaly_direction": get_value("anomaly_direction"),
        "time_bucket": get_value("time_bucket"),
        "days_back": get_value("training_period")["count"] * days_back_multiplier,
        "backfill_days": get_value("detection_period")["count"],
        "seasonality": get_value("seasonality"),
        "event_timestamp_column": get_value("event_timestamp_column"),
        "ignore_small_changes": get_value("ignore_small_changes"),
        "fail_on_zero": get_value("fail_on_zero"),
        "detection_delay": get_value("detection_delay"),
        "anomaly_exclude_metrics": get_value("anomaly_exclude_metrics"),
        "freshness_column": None,  # Deprecated
        "dimensions": None,  # should only be set at the test level,
        "exclude_final_results": get_value("exclude_final_results"),
    }


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="vars",
    vars_config={key: value.vars for key, value in PARAM_VALUES.items()},
    model_config={},
    test_config={},
    expected_config=_get_expected_adapted_config("vars"),
)
@Parametrization.case(
    name="model",
    vars_config={key: value.vars for key, value in PARAM_VALUES.items()},
    model_config={key: value.model for key, value in PARAM_VALUES.items()},
    test_config={},
    expected_config=_get_expected_adapted_config("model"),
)
@Parametrization.case(
    name="test",
    vars_config={key: value.vars for key, value in PARAM_VALUES.items()},
    model_config={key: value.model for key, value in PARAM_VALUES.items()},
    test_config={key: value.test for key, value in PARAM_VALUES.items()},
    expected_config=_get_expected_adapted_config("test"),
)
@pytest.mark.skip_targets(["clickhouse"])
@pytest.mark.skip_for_dbt_fusion
def test_anomaly_test_configuration(
    dbt_project: DbtProject,
    vars_config: dict,
    model_config: dict,
    test_config: dict,
    expected_config: dict,
):
    dbt_project.dbt_runner.vars.update(vars_config)
    result = dbt_project.dbt_runner.run_operation(
        "elementary_tests.get_anomaly_config",
        macro_args={"model_config": model_config, "config": test_config},
    )
    adapted_config = json.loads(result[0])
    assert adapted_config == expected_config
