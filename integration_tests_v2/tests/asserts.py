import json
from typing import List, Optional

import dbt_project


def read_table(
    table_name: str,
    where: Optional[str] = None,
    column_names: Optional[List[str]] = None,
) -> List[dict]:
    return json.loads(
        dbt_project.get_dbt_runner().run_operation(
            "read_table",
            macro_args={
                "table": table_name,
                "where": where,
                "column_names": column_names,
            },
        )[0]
    )
