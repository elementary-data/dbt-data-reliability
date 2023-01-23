import uuid
from typing import Dict, List, Any

import agate
from dbt.adapters.base import BaseRelation

from .dbt_project import DbtProject


def create_test_table(dbt_project: DbtProject, name: str, columns: Dict[str, str]) -> BaseRelation:
    identifier = f"{name}_{uuid.uuid4().hex[:5]}"
    relation = dbt_project.create_relation(None, None, identifier)

    empty_table_query = dbt_project.execute_macro(
        "elementary.empty_table",
        column_name_and_type_list=list(columns.items())
    )
    create_table_query = dbt_project.execute_macro(
        "dbt.create_table_as",
        temporary=True,
        relation=relation,
        compiled_code=empty_table_query
    )

    dbt_project.execute_sql(create_table_query)

    return relation


def insert_rows(dbt_project: DbtProject, relation: BaseRelation, rows: List[Dict]):
    dbt_project.execute_macro(
        "elementary.insert_rows",
        table_relation=relation,
        rows=rows,
        should_commit=True
    )


def update_var(dbt_project: DbtProject, var_name: str, var_value: Any):
    dbt_project.config.vars.vars[var_name] = var_value


def lowercase_column_names(table: agate.table.Table):
    return table.rename(column_names={col: col.lower() for col in table.column_names})
