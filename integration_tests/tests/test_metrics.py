import random
from datetime import datetime

from data_generator import DATE_FORMAT, generate_dates
from dbt_project import DbtProject


def test_metrics(dbt_project: DbtProject):
    now = datetime.utcnow()
    dbt_project.dbt_runner.vars["collect_metrics"] = True
    data1 = [
        {"updated_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=now)
        for _ in range(random.randint(-5, 20))
    ]
    data2 = [
        {"created_at": date.strftime(DATE_FORMAT)}
        for date in generate_dates(base_date=now)
        for _ in range(random.randint(0, 20))
    ]
    dbt_project.seed(data1, "metrics_seed1")
    dbt_project.seed(data2, "metrics_seed2")
    dbt_project.dbt_runner.run(select="metrics")

    remaining_models_to_row_count = {
        "metrics_table": len(data1),
        "metrics_incremental": len(data2),
    }
    for metric in dbt_project.read_table("data_monitoring_metrics"):
        for model_name, row_count in remaining_models_to_row_count.items():
            if model_name.upper() in metric["full_table_name"]:
                if metric["metric_name"] == "row_count":
                    assert metric["metric_value"] == row_count
                elif metric["metric_name"] == "build_timestamp":
                    assert metric["metric_value"] > now.timestamp()
                remaining_models_to_row_count.pop(model_name)
                break

    assert not remaining_models_to_row_count
