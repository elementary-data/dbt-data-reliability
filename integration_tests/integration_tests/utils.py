import uuid
from typing import Dict, List, Any

import agate
from dbt.adapters.base import BaseRelation
from dbt_osmosis.core.osmosis import DbtProject


def create_test_table(dbt_project: DbtProject, name: str, columns: Dict[str, str]) -> BaseRelation:
    identifier = f"{name}_{uuid.uuid4().hex[:5]}"
    relation = dbt_project.create_relation(None, None, identifier)

    # (note - dbt-osmosis API doesn't support passing the project right now)
    empty_table_query = dbt_project.execute_macro(
        "empty_table",
        kwargs={"column_name_and_type_list": list(columns.items())}
    )
    create_table_query = dbt_project.execute_macro(
        "create_table_as",
        kwargs={"temporary": True,
                "relation": relation,
                "compiled_code": empty_table_query}
    )

    dbt_project.execute_sql(create_table_query)

    return relation


def insert_rows(dbt_project: DbtProject, relation: BaseRelation, rows: List[Dict]):
    dbt_project.execute_macro("insert_rows", kwargs={"table_relation": relation,
                                                     "rows": rows,
                                                     "should_commit": True})


def render_jinja(dbt_project: DbtProject, jinja_str: str):
    return dbt_project.compile_sql(jinja_str).compiled_sql


def update_var(dbt_project: DbtProject, var_name: str, var_value: Any):
    dbt_project.config.vars.vars[var_name] = var_value


def lowercase_column_names(table: agate.table.Table):
    return table.rename(column_names={col: col.lower() for col in table.column_names})
