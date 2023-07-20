import random
from datetime import datetime

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject


def test_metrics(dbt_project: DbtProject):
    data1 = [
        {"updated_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(random.randint(-5, 20))
    ]
    data2 = [
        {"created_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=datetime.now())
        for _ in range(random.randint(0, 20))
    ]
    with dbt_project.seed(data1, "first_metrics_table_seed"):
        pass
    with dbt_project.seed(data2, "second_metrics_table_seed"):
        pass

    dbt_project.dbt_runner.run(select="tag:metrics")

    first_metric_found = False
    second_metric_found = False
    for metric in dbt_project.read_table("data_monitoring_metrics"):
        if (
            "first_metrics_table" in metric["full_table_name"]
            and metric["metric_name"] == "row_count"
        ):
            assert metric["metric_value"] == len(data1)
            first_metric_found = True
        if (
            "second_metrics_table" in metric["full_table_name"]
            and metric["metric_name"] == "row_count"
        ):
            assert metric["metric_value"] == len(data2)
            second_metric_found = True

    assert first_metric_found and second_metric_found
