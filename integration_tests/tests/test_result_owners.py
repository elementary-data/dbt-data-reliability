import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

COLUMN_NAME = "country"
DEFAULT_OWNER = "central-team"
MODEL_CONFIG = {"config": {"meta": {"owner": DEFAULT_OWNER}}}
ACCEPTED_VALUES_ARGS = {"values": ["NL"]}
TIMESTAMP_COLUMN = "updated_at"


def _owners(test_result: Dict[str, Any]) -> List[str]:
    owners = test_result["owners"]
    return json.loads(owners) if isinstance(owners, str) else owners


def _accepted_values_test(
    test_id: str,
    dbt_project: DbtProject,
    data: List[dict],
    result_owners: list,
    test_config: Optional[Dict[str, Any]] = None,
):
    return dbt_project.test(
        test_id,
        "accepted_values",
        ACCEPTED_VALUES_ARGS,
        test_column=COLUMN_NAME,
        data=data,
        model_config=MODEL_CONFIG,
        test_config={**(test_config or {}), "meta": {"result_owners": result_owners}},
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


def test_matched_owners_are_sorted(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: "JP"}]
    result_owners = [
        {"expression": "1 = 1", "owners": ["b@example.com", "a@example.com"]}
    ]
    test_result = _accepted_values_test(test_id, dbt_project, data, result_owners)
    assert _owners(test_result) == ["a@example.com", "b@example.com"]


def test_warn_status_gets_result_owners(test_id: str, dbt_project: DbtProject):
    data = [{COLUMN_NAME: value} for value in ["NL", "JP"]]
    result_owners = [
        {"expression": "value_field = 'JP'", "owners": ["japan@example.com"]}
    ]
    test_result = _accepted_values_test(
        test_id, dbt_project, data, result_owners, test_config={"severity": "warn"}
    )
    assert test_result["status"] == "warn"
    assert _owners(test_result) == ["japan@example.com"]


def _dimension_data(
    test_day_rows: List[Dict[str, Any]], training_rows: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))
    return [
        {TIMESTAMP_COLUMN: test_date.strftime(DATE_FORMAT), **row}
        for row in test_day_rows
    ] + [
        {TIMESTAMP_COLUMN: cur_date.strftime(DATE_FORMAT), **row}
        for cur_date in training_dates
        for row in training_rows
    ]


def test_dimension_anomalies_partial_match(test_id: str, dbt_project: DbtProject):
    # Both dimension values are anomalous, but only one has a rule.
    data = _dimension_data(
        [{"superhero": "Superman"}] * 3 + [{"superhero": "Spiderman"}] * 3,
        [{"superhero": "Superman"}, {"superhero": "Spiderman"}],
    )
    result_owners = [
        {"expression": "dimension_value = 'Superman'", "owners": ["dc@example.com"]}
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


def test_multi_column_dimension_result_owners(test_id: str, dbt_project: DbtProject):
    # Multi-column dimension values are joined with "; ".
    data = _dimension_data(
        [{"universe": "DC", "superhero": "Superman"}] * 3
        + [{"universe": "Marvel", "superhero": "Spiderman"}],
        [
            {"universe": "DC", "superhero": "Superman"},
            {"universe": "Marvel", "superhero": "Spiderman"},
        ],
    )
    result_owners = [
        {
            "expression": "dimension_value = 'DC; Superman'",
            "owners": ["dc@example.com"],
        },
        {
            "expression": "dimension_value = 'Marvel; Spiderman'",
            "owners": ["marvel@example.com"],
        },
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.dimension_anomalies",
        {
            "timestamp_column": TIMESTAMP_COLUMN,
            "dimensions": ["universe", "superhero"],
        },
        data=data,
        model_config=MODEL_CONFIG,
        test_config={"meta": {"result_owners": result_owners}},
    )
    assert test_result["status"] == "fail"
    assert _owners(test_result) == ["dc@example.com"]


def test_column_anomalies_result_owners_per_metric(
    test_id: str, dbt_project: DbtProject
):
    # One result per metric: each result only gets the owners of its own rows.
    data = _dimension_data(
        [{"superhero": None}] * 3,
        [{"superhero": "Superman"}, {"superhero": "Batman"}],
    )
    result_owners = [
        {"expression": "metric_name = 'null_count'", "owners": ["nulls@example.com"]}
    ]
    test_results = dbt_project.test(
        test_id,
        "elementary.column_anomalies",
        {
            "timestamp_column": TIMESTAMP_COLUMN,
            "column_anomalies": ["null_count", "null_percent"],
        },
        data=data,
        test_column="superhero",
        model_config=MODEL_CONFIG,
        test_config={"meta": {"result_owners": result_owners}},
        multiple_results=True,
    )
    owners_by_metric = {
        result["test_sub_type"]: _owners(result) for result in test_results
    }
    assert {result["status"] for result in test_results} == {"fail"}
    assert owners_by_metric == {
        "null_count": ["nulls@example.com"],
        "null_percent": [DEFAULT_OWNER],
    }


def test_volume_anomalies_result_owners(test_id: str, dbt_project: DbtProject):
    data = _dimension_data([{}] * 6, [{}])
    result_owners = [
        {"expression": "metric_name = 'row_count'", "owners": ["volume@example.com"]}
    ]
    test_result = dbt_project.test(
        test_id,
        "elementary.volume_anomalies",
        {"timestamp_column": TIMESTAMP_COLUMN},
        data=data,
        model_config=MODEL_CONFIG,
        test_config={"meta": {"result_owners": result_owners}},
    )
    assert test_result["status"] == "fail"
    assert _owners(test_result) == ["volume@example.com"]
