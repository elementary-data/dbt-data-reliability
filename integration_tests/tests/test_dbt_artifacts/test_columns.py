import json
from typing import List, Optional

from dbt_project import DbtProject
from parametrization import Parametrization

TABLE_NODE = {
    "columns": {
        "with_description": {
            "name": "with_description",
            "description": "This column has a description",
        },
        "without_description": {
            "name": "without_description",
        },
        "with_empty_description": {
            "name": "with_empty_description",
            "description": "",
        },
        "with_null_description": {
            "name": "with_null_description",
            "description": None,
        },
    }
}


@Parametrization.autodetect_parameters()
@Parametrization.case(
    name="default",
    columns_upload_strategy=None,
    expected_columns=["with_description"],
)
@Parametrization.case(
    name="only_with_description",
    columns_upload_strategy="enriched_only",
    expected_columns=["with_description"],
)
@Parametrization.case(
    name="all",
    columns_upload_strategy="all",
    expected_columns=[
        "with_description",
        "without_description",
        "with_empty_description",
        "with_null_description",
    ],
)
def test_flatten_table_columns(
    dbt_project: DbtProject,
    columns_upload_strategy: Optional[str],
    expected_columns: List[str],
) -> None:
    if columns_upload_strategy is not None:
        dbt_project.dbt_runner.vars["columns_upload_strategy"] = columns_upload_strategy
    flattened_columns = json.loads(
        dbt_project.dbt_runner.run_operation(
            "elementary.flatten_table_columns", macro_args={"table_node": TABLE_NODE}
        )[0]
    )
    flattened_column_names = [column["name"] for column in flattened_columns]
    assert set(flattened_column_names) == set(expected_columns)
