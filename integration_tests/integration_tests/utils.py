import uuid
from functools import lru_cache
from typing import Dict, List, Any
from packaging import version

import agate
from dbt.adapters.base import BaseRelation

from .dbt_project import DbtProject, dbt_version


def create_test_table(dbt_project: DbtProject, name: str, columns: Dict[str, str]) -> BaseRelation:
    identifier = f"{name}_{uuid.uuid4().hex[:5]}"

    temporary = (dbt_project.adapter_name != "databricks")
    if dbt_project.adapter_name not in ["postgres", "redshift"]:
        database, schema = get_package_database_and_schema(dbt_project)
    else:
        database = schema = None

    relation = dbt_project.create_relation(database, schema, identifier)

    empty_table_query = dbt_project.execute_macro(
        "elementary.empty_table",
        column_name_and_type_list=list(columns.items())
    )

    create_table_kwargs = {
        "temporary": temporary,
        "relation": relation
    }
    if dbt_version >= version.parse("1.3.0"):
        create_table_kwargs["compiled_code"] = empty_table_query
    else:
        create_table_kwargs["sql"] = empty_table_query

    create_table_query = dbt_project.execute_macro("dbt.create_table_as", **create_table_kwargs)

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


@lru_cache()
def get_package_database_and_schema(dbt_project):
    return dbt_project.execute_macro("elementary.get_package_database_and_schema")

