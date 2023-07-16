import json
from typing import List, Optional

import dbt_project


def read_table(
    table_name: str,
    where: Optional[str] = None,
    column_names: Optional[List[str]] = None,
    raise_if_empty: bool = True,
) -> List[dict]:
    results = json.loads(
        dbt_project.get_dbt_runner().run_operation(
            "read_table",
            macro_args={
                "table": table_name,
                "where": where,
                "column_names": column_names,
            },
        )[0]
    )
    if raise_if_empty and len(results) == 0:
        raise ValueError(f"Table '{table_name}' with the '{where}' condition is empty.")
    return results
