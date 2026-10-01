import json
from datetime import datetime, timedelta
from typing import Any, Dict, List

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

COLUMN_NAME = "country"
DEFAULT_OWNER = "central-team"
MODEL_CONFIG = {"meta": {"owner": DEFAULT_OWNER}}
ACCEPTED_VALUES_ARGS = {"values": ["NL"]}
TIMESTAMP_COLUMN = "updated_at"


def _owners(test_result: Dict[str, Any]) -> List[str]:
    owners = test_result["owners"]
    return json.loads(owners) if isinstance(owners, str) else owners


def _accepted_values_test(
    test_id: str, dbt_project: DbtProject, data: List[dict], result_owners: list
):
    return dbt_project.test(
        test_id,
        "accepted_values",
        ACCEPTED_VALUES_ARGS,
        test_column=COLUMN_NAME,
        data=data,
        model_config=MODEL_CONFIG,
        test_config={"meta": {"result_owners": result_owners}},
        test_vars={"enable_elementary_test_materialization": True},
    )


def test_matching_rules_union_owners(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: value} for value in ["NL", "JP", "JP", "KR"]]
    result_owners = [
        {"expression": "value_field = 'JP'", "owners": ["japan@example.com"]},
        {
            "expression": "value_field in ('JP', 'KR')",
            "owners": "japan@example.com, korea@example.com",
        },
        {"expression": "value_field = 'TW'", "owners": ["taiwan@example.com"]},
    ]
    test_result = _accepted_values_test(test_id, dbt_project, data, result_owners)
    assert test_result["status"] == "fail"
    assert _owners(test_result) == ["japan@example.com", "korea@example.com"]


def test_no_matching_rule_keeps_default_owners(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: value} for value in ["NL", "JP"]]
    result_owners = [
        {"expression": "value_field = 'TW'", "owners": ["taiwan@example.com"]}
    ]
    test_result = _accepted_values_test(test_id, dbt_project, data, result_owners)
    assert test_result["status"] == "fail"
    assert _owners(test_result) == [DEFAULT_OWNER]


def test_passing_test_keeps_default_owners(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: "NL"}]
    result_owners = [{"expression": "1 = 1", "owners": ["everyone@example.com"]}]
    test_result = _accepted_values_test(test_id, dbt_project, data, result_owners)
    assert test_result["status"] == "pass"
    assert _owners(test_result) == [DEFAULT_OWNER]


def test_dimension_anomalies_result_owners(test_id: str, dbt_project: DbtProject):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    data: List[Dict[str, Any]] = [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), "superhero": superhero}
        for superhero in ["Superman", "Superman", "Superman", "Spiderman"]
    ] + [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), "superhero": superhero}
        for cur_date in training_dates
        for superhero in ["Superman", "Spiderman"]
    ]
    result_owners = [
        {"expression": "dimension_value = 'Superman'", "owners": ["dc@example.com"]},
        {
            "expression": "dimension_value = 'Spiderman'",
            "owners": ["marvel@example.com"],
        },
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.dimension_anomalies",
        {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": ["superhero"]},
        data=data,
        model_config=MODEL_CONFIG,
        test_config={"meta": {"result_owners": result_owners}},
    )
    assert test_result["status"] == "fail"
    assert _owners(test_result) == ["dc@example.com"]
