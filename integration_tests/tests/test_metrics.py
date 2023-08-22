import random
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import pytest
from dbt_project import DbtProject


@dataclass
class MetricSeedTable:
    data: List[dict]
    table_name: str


def test_metrics_sql_models(dbt_project: DbtProject):
    seed_tables = [
        MetricSeedTable(
            data=[{"hello": "world"} for _ in range(random.randint(5, 10))],
            table_name="metrics_seed1",
        ),
        MetricSeedTable(
            data=[{"hello": "world"} for _ in range(random.randint(5, 20))],
            table_name="metrics_seed2",
        ),
    ]
    for seed_table in seed_tables:
        dbt_project.seed(seed_table.data, seed_table.table_name)

    dbt_project.dbt_runner.run(
        select="models/metrics/sql", vars={"collect_metrics": True}
    )
    remaining_models_to_row_count = {
        "metrics_table": len(seed_tables[0].data),
        "metrics_incremental": len(seed_tables[1].data),
    }
    validate_metrics(dbt_project, remaining_models_to_row_count)


@pytest.mark.requires_dbt_version("1.3.0")
@pytest.mark.only_on_targets(["snowflake"])
def test_metrics_python_models(dbt_project: DbtProject):
    seed_table = MetricSeedTable(
        data=[{"hello": "world"} for _ in range(random.randint(5, 10))],
        table_name="metrics_seed3",
    )
    dbt_project.seed(seed_table.data, seed_table.table_name)
    dbt_project.dbt_runner.run(
        select="models/metrics/python", vars={"collect_metrics": True}
    )
    remaining_models_to_row_count = {"metrics_python_table": len(seed_table.data)}
    validate_metrics(dbt_project, remaining_models_to_row_count)


def validate_metrics(
    dbt_project: DbtProject,
    remaining_models_to_row_count: dict,
):
    yesterday = datetime.utcnow() - timedelta(days=1)
    for metric in dbt_project.read_table("data_monitoring_metrics"):
        for model_name, row_count in remaining_models_to_row_count.items():
            if model_name.upper() in metric["full_table_name"]:
                if metric["metric_name"] == "row_count":
                    assert metric["metric_value"] == row_count
                elif metric["metric_name"] == "build_timestamp":
                    assert metric["metric_value"] > yesterday.timestamp()
                remaining_models_to_row_count.pop(model_name)
                break

    assert not remaining_models_to_row_count
