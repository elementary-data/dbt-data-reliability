import os
from typing import Dict

import pytest
import uuid

from .dbt_project import DbtProject
from .utils import insert_rows, create_test_table

from elementary.clients.dbt.dbt_runner import DbtRunner


PROJECT_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
MODELS_DIR = os.path.join(
    PROJECT_DIR, "models"
)


@pytest.mark.integration
def test_test_volume_anomalies(dbt_target, dbt_project, dbt_project_dir, elementary_schema):
    # dbt_project is a slim runner for running operations
    schema_name = elementary_schema.schema # 'roi_elementary'
    columns_to_dtypes = {"updated_at": "datetime"}

    csv_pth = os.path.join(PROJECT_DIR, "data", "training", "daily_wh_activity_training.csv")
    model_base_name = "my_model"
    print(f"creating test table relation")
    table_relation = create_test_table(dbt_project, name=model_base_name,columns=columns_to_dtypes)
    unique_table_name = table_relation.identifier
    print(f"create table {unique_table_name}")
    print(csv_pth)
    insert_rows(dbt_project=dbt_project,
                relation = table_relation,
                rows = csv_pth)

    dbt_runner = DbtRunner(project_dir=PROJECT_DIR, target=dbt_target)  # dbt_runner is a heavy runner for running tests
    success, output = dbt_runner.build(select="dummy_model",
                                       vars={"integration_tests_schema": schema_name,
                                             "integration_tests_unique_table_name": unique_table_name,
                                             "integration_tests_volume_anomalies_timestamp_column": 'updated_at',
                                             "integration_tests_volume_anomalies_sensitivity": 1})
    import pdb; pdb.set_trace()

    assert True
