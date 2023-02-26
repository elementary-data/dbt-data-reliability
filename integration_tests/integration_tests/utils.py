import uuid
from functools import lru_cache
from typing import Dict, List, Any, Union
import csv
import pandas as pd

import agate
from dbt.adapters.base import BaseRelation

from .dbt_project import DbtProject, dbt_version


def create_test_table(
    dbt_project: DbtProject, name: str, columns: Dict[str, str]
) -> BaseRelation:
    identifier = f"{name}_{uuid.uuid4().hex[:5]}"

    if dbt_project.adapter_name not in ["postgres", "redshift"]:
        database, schema = get_package_database_and_schema(dbt_project)
    else:
        database = schema = None

    relation = dbt_project.create_relation(database, schema, identifier)

    empty_table_query = dbt_project.execute_macro(
        "elementary.empty_table", column_name_and_type_list=list(columns.items())
    )

    dbt_project.create_table_as(relation, empty_table_query, temporary=True)

    return relation


def insert_rows_from_list_of_dicts(
    dbt_project: DbtProject, relation: BaseRelation, rows: List[Dict]
):
    dbt_project.execute_macro(
        "elementary.insert_rows", table_relation=relation, rows=rows, should_commit=True
    )


def insert_rows_from_csv(
    dbt_project: DbtProject, relation: BaseRelation, rows_path: str
):
    def fillna(row):
        d = {k: (None if v == "" else v) for (k, v) in row.items()}
        return d

    def convert_numeric_columns(
        row, numeric_columns=["metric_value", "bucket_duration_hours"]
    ):
        d = {
            k: eval(v) if (v and (k in numeric_columns)) else v
            for (k, v) in row.items()
        }
        return d

    with open(rows_path) as rows_csv:
        reader = csv.DictReader(rows_csv)
        rows = [convert_numeric_columns(fillna(row)) for row in reader]
        insert_rows_from_list_of_dicts(dbt_project, relation, rows)


def insert_rows(
    dbt_project: DbtProject, relation: BaseRelation, rows: Union[str, List[Dict]]
):
    if isinstance(rows, str):
        insert_rows_from_csv(dbt_project, relation, rows)
    elif isinstance(rows, List):
        insert_rows_from_list_of_dicts(dbt_project, relation, rows)
    else:
        raise ValueError(
            f"Got rows as {type(rows)}, should be either string [csv file path] or List of dictionaries "
        )


def update_var(dbt_project: DbtProject, var_name: str, var_value: Any):
    dbt_project.config.vars.vars[var_name] = var_value


def lowercase_column_names(table: agate.table.Table):
    return table.rename(column_names={col: col.lower() for col in table.column_names})


@lru_cache()
def get_package_database_and_schema(dbt_project):
    return dbt_project.execute_macro("elementary.get_package_database_and_schema")


def agate_table_to_pandas_dataframe(table):
    temp_dict = {}
    for ix, row in enumerate(table.rows):
        temp_dict[ix] = row
    df = pd.DataFrame.from_dict(temp_dict, orient="index")
    df.columns = table.column_names
    return df


def assert_dfs_equal(
    df, df2, columns_to_ignore, column_to_index_by, datetime_columns, numeric_columns
):
    for col in datetime_columns:
        df[col] = pd.to_datetime(df[col], format="%Y-%m-%dT%H:%M:%S").dt.tz_localize(
            None
        )
        df2[col] = pd.to_datetime(df2[col], format="%Y-%m-%dT%H:%M:%S").dt.tz_localize(
            None
        )
    for col in numeric_columns:
        df[col] = pd.to_numeric(df[col]).astype("float64")
        df2[col] = pd.to_numeric(df2[col]).astype("float64")
    df = df.set_index(column_to_index_by).drop(columns_to_ignore, axis=1)
    df2 = df2.set_index(column_to_index_by).drop(columns_to_ignore, axis=1)
    pd.testing.assert_frame_equal(df, df2, check_column_type=False)
