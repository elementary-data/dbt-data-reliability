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
    only_with_description=None,
    expected_columns=["with_description"],
)
@Parametrization.case(
    name="only_with_description",
    only_with_description=True,
    expected_columns=["with_description"],
)
@Parametrization.case(
    name="all",
    only_with_description=False,
    expected_columns=[
        "with_description",
        "without_description",
        "with_empty_description",
        "with_null_description",
    ],
)
def test_flatten_table_columns(
    dbt_project: DbtProject,
    only_with_description: Optional[bool],
    expected_columns: List[str],
) -> None:
    if only_with_description is not None:
        dbt_project.dbt_runner.vars[
            "upload_only_columns_with_descriptions"
        ] = only_with_description
    flattened_columns = json.loads(
        dbt_project.dbt_runner.run_operation(
            "elementary.flatten_table_columns", macro_args={"table_node": TABLE_NODE}
        )[0]
    )
    flattened_column_names = [column["name"] for column in flattened_columns]
    assert flattened_column_names == expected_columns
