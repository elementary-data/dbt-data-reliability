from datetime import datetime, timedelta

from asserts import read_table
from data_generator import DATE_FORMAT, generate_dates
from run_dbt_test import run_dbt_test


def read_status(test_id):
    results = read_table(
        "elementary_test_results",
        where=f"table_name = '{test_id}'",
        column_names=["status"],
    )
    return results


def test_anomalyless_table_volume_anomalies(request):
    test_id = request.node.name
    dbt_test_name = "elementary.volume_anomalies"
    data = [
        {"updated_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now(), days_back=120)
        for _ in range(100)
    ]
    run_dbt_test(data, test_id, dbt_test_name, {"timestamp_column": "updated_at"})
    assert all(result["status"] == "pass" for result in read_status(test_id))


def test_anomalyful_table_volume_anomalies(request):
    test_id = request.node.name
    dbt_test_name = "elementary.volume_anomalies"
    data = [
        {"updated_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now(), days_back=120)
        for _ in range(100)
        if date < datetime.now() - timedelta(days=2)
    ]
    run_dbt_test(data, test_id, dbt_test_name, {"timestamp_column": "updated_at"})
    assert all(result["status"] == "fail" for result in read_status(test_id))
