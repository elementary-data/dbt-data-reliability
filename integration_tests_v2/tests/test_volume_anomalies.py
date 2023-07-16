from datetime import datetime

from asserts import read_table
from data_generator import DATE_FORMAT, generate_dates
from run_dbt_test import run_dbt_test


def test_table_volume_anomalies():
    table_name = "volume_anomalies"
    test_name = "elementary.volume_anomalies"
    data = [
        {"updated_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now(), days_back=120)
        for _ in range(100)
    ]
    run_dbt_test(data, table_name, test_name, {"timestamp_column": "updated_at"})
    results = read_table(
        "elementary_test_results",
        where=f"test_alias = '{test_name}'",
        column_names=["status"],
    )
    assert all(result["status"] == "fail" for result in results)
