import json
from datetime import date, datetime, timedelta
from typing import List, Optional, Sequence, Tuple, Union

import pytest
from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject

TIMESTAMP_COLUMN = "updated_at"
NESTED_COLUMN = "user_info.address.city"
PLAIN_COLUMN = "superhero"
COLUMN_TEST_NAME = "elementary.column_anomalies"
DIMENSION_TEST_NAME = "elementary.dimension_anomalies"

# Nested STRUCT leaves are only supported on BigQuery.
SUPPORTED_TARGETS = ["bigquery"]

# (updated_at, superhero, user_info.address.city)
Row = Tuple[Union[date, datetime], str, Optional[str]]


def _row_sql(updated_at: Union[date, datetime], superhero: str, city: Optional[str]):
    city_sql = "cast(null as string)" if city is None else f"cast('{city}' as string)"
    return (
        f"select timestamp '{updated_at.strftime(DATE_FORMAT)}' as {TIMESTAMP_COLUMN}"
        f", cast('{superhero}' as string) as {PLAIN_COLUMN}"
        ", struct("
        f"struct({city_sql} as city, cast('US' as string) as country) as address"
        ", cast('hero' as string) as name"
        ") as user_info"
        # A REPEATED leaf and a REPEATED ancestor, so that nested-column
        # discovery has to skip fields that would require UNNEST rather than
        # generating invalid SQL for them.
        ", [struct(cast(1 as int64) as amount)] as orders"
        ", ['tag'] as tags"
    )


def _create_struct_model(dbt_project: DbtProject, test_id: str, rows: Sequence[Row]):
    """Materialize a table with nested STRUCT columns, then leave it in place.

    ``DbtProject.test(as_model=True)`` re-creates a dummy model file with the
    same name so the node exists in the manifest; the physical table built here
    is what the test actually reads.
    """
    query = "\nunion all\n".join(_row_sql(*row) for row in rows)
    with dbt_project.create_temp_model_for_existing_table(
        test_id, materialization="table", raw_code=query
    ) as model_path:
        assert dbt_project.dbt_runner.run(
            select=str(model_path)
        ), "Failed to build the nested STRUCT model"


def _stable_rows(base_date) -> List[Row]:
    return [
        (cur_date, superhero, city)
        for cur_date in generate_dates(base_date=base_date)
        for superhero, city in [("Superman", "Metropolis"), ("Batman", "Gotham")]
    ]


def _anomaly_test_points(dbt_project: DbtProject, test_id: str):
    results = dbt_project.run_query(dbt_project.samples_query(test_id))
    return [json.loads(result["result_row"]) for result in results]


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_anomalyless_column_anomalies_on_struct_field(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    _create_struct_model(dbt_project, test_id, _stable_rows(utc_today - timedelta(1)))

    test_result = dbt_project.test(
        test_id,
        COLUMN_TEST_NAME,
        {"timestamp_column": TIMESTAMP_COLUMN, "column_anomalies": ["null_count"]},
        test_column=NESTED_COLUMN,
        as_model=True,
    )
    assert test_result["status"] == "pass"
    # The dotted path is what alerts display, so it must survive into the results.
    assert test_result["column_name"].lower() == NESTED_COLUMN


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_anomalous_column_anomalies_on_struct_field(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    rows: List[Row] = [(test_date, "Superman", None) for _ in range(3)]
    rows += [
        (cur_date, superhero, city)
        for cur_date in training_dates
        for superhero, city in [("Superman", "Metropolis"), ("Batman", "Gotham")]
    ]
    _create_struct_model(dbt_project, test_id, rows)

    test_result = dbt_project.test(
        test_id,
        COLUMN_TEST_NAME,
        {"timestamp_column": TIMESTAMP_COLUMN, "column_anomalies": ["null_count"]},
        test_column=NESTED_COLUMN,
        as_model=True,
    )
    assert test_result["status"] == "fail"


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_column_anomalies_on_repeated_descendant_rejected(
    test_id: str, dbt_project: DbtProject
):
    """Leaves under a REPEATED ancestor need UNNEST, so discovery must exclude
    them and the test must error at the column lookup rather than generate
    invalid SQL."""
    utc_today = datetime.utcnow().date()
    _create_struct_model(dbt_project, test_id, _stable_rows(utc_today - timedelta(1)))

    test_result = dbt_project.test(
        test_id,
        COLUMN_TEST_NAME,
        {"timestamp_column": TIMESTAMP_COLUMN, "column_anomalies": ["null_count"]},
        test_column="orders.amount",
        as_model=True,
    )
    assert test_result["status"] == "error"
    assert "unable to find column" in test_result["test_results_description"].lower()


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_column_anomalies_with_struct_dimension(test_id: str, dbt_project: DbtProject):
    """Plain monitored column, nested STRUCT leaf as the dimension."""
    utc_today = datetime.utcnow().date()
    _create_struct_model(dbt_project, test_id, _stable_rows(utc_today - timedelta(1)))

    test_result = dbt_project.test(
        test_id,
        COLUMN_TEST_NAME,
        {
            "timestamp_column": TIMESTAMP_COLUMN,
            "column_anomalies": ["null_count"],
            "dimensions": [NESTED_COLUMN],
        },
        test_column=PLAIN_COLUMN,
        as_model=True,
    )
    assert test_result["status"] == "pass"

    points = _anomaly_test_points(dbt_project, test_id)
    assert points, "No metric data points were collected"
    # The dimension must resolve to the STRUCT leaf's values, not to nulls.
    assert {point["dimension"] for point in points} == {NESTED_COLUMN}
    assert {point["dimension_value"] for point in points} == {"Metropolis", "Gotham"}


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_column_anomalies_on_struct_field_with_struct_dimension(
    test_id: str, dbt_project: DbtProject
):
    """Both the monitored column and the dimension are nested STRUCT leaves."""
    utc_today = datetime.utcnow().date()
    _create_struct_model(dbt_project, test_id, _stable_rows(utc_today - timedelta(1)))

    test_result = dbt_project.test(
        test_id,
        COLUMN_TEST_NAME,
        {
            "timestamp_column": TIMESTAMP_COLUMN,
            "column_anomalies": ["null_count"],
            "dimensions": [NESTED_COLUMN],
        },
        test_column=NESTED_COLUMN,
        as_model=True,
    )
    assert test_result["status"] == "pass"
    assert test_result["column_name"].lower() == NESTED_COLUMN


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_anomalyless_dimension_anomalies_on_struct_field(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    _create_struct_model(dbt_project, test_id, _stable_rows(utc_today - timedelta(1)))

    test_result = dbt_project.test(
        test_id,
        DIMENSION_TEST_NAME,
        {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": [NESTED_COLUMN]},
        as_model=True,
    )
    assert test_result["status"] == "pass"


@pytest.mark.only_on_targets(SUPPORTED_TARGETS)
def test_anomalous_dimension_anomalies_on_struct_field(
    test_id: str, dbt_project: DbtProject
):
    utc_today = datetime.utcnow().date()
    test_date, *training_dates = generate_dates(base_date=utc_today - timedelta(1))

    rows: List[Row] = [
        (test_date, superhero, city)
        for superhero, city in [
            ("Superman", "Metropolis"),
            ("Superman", "Metropolis"),
            ("Superman", "Metropolis"),
            ("Batman", "Gotham"),
        ]
    ]
    rows += [
        (cur_date, superhero, city)
        for cur_date in training_dates
        for superhero, city in [("Superman", "Metropolis"), ("Batman", "Gotham")]
    ]
    _create_struct_model(dbt_project, test_id, rows)

    test_result = dbt_project.test(
        test_id,
        DIMENSION_TEST_NAME,
        {"timestamp_column": TIMESTAMP_COLUMN, "dimensions": [NESTED_COLUMN]},
        as_model=True,
    )
    assert test_result["status"] == "fail"

    points = _anomaly_test_points(dbt_project, test_id)
    # Only anomalous dimension values are stored for dimension anomalies.
    assert {point["dimension_value"] for point in points} == {"Metropolis"}
    assert any(point["is_anomalous"] for point in points)
