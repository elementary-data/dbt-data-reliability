from datetime import datetime
import os
import pandas as pd
from parametrization import Parametrization


from .dbt_project import DbtProject
from .utils import (
    create_test_table,
    insert_rows,
    update_var,
    lowercase_column_names,
    assert_dfs_equal,
    agate_table_to_pandas_dataframe,
    get_package_database_and_schema,
)

MIN_BUCKET_START = datetime(2022, 1, 1, 0, 0, 0)
RUN_STARTED_AT = datetime(2022, 1, 4, 0, 13, 42)
DATA_MONITORING_METRICS_COLUMNS_TO_TYPES = [
    ("id", "string"),  # same as empty_table.sql::empty_data_monitoring_metrics
    ("full_table_name", "string"),
    ("column_name", "string"),
    ("metric_name", "string"),
    ("metric_value", "float"),
    ("source_value", "string"),
    ("bucket_start", "timestamp"),
    ("bucket_end", "timestamp"),
    ("bucket_duration_hours", "int"),
    ("updated_at", "timestamp"),
    ("dimension", "string"),
    ("dimension_value", "string"),
    ("metric_properties", "string"),
]
BASE_DIR = os.path.join(
    os.path.dirname(os.path.realpath(__file__)), "anomaly_query_cases"
)


@Parametrization.autodetect_parameters()
@Parametrization.default_parameters(
    time_bucket={"period": "day", "count": 1},
    timestamp_column="updated_at",
    days_back=30,
)
@Parametrization.case(
    name="freshness_1_day_mostly_non_anomalous",
    metric_name="freshness",
    input_rows=os.path.join(BASE_DIR, "freshness_1_day_test.csv"),
    input_rows_history=os.path.join(
        BASE_DIR, "freshness_1_day_history.csv"
    ),
    expected_output=os.path.join(
        BASE_DIR, "freshness_1_day_after.csv"
    ),
    alias_table_name="numeric_column_anomalies",
)
@Parametrization.case(
    name="row_count_1_day_1_row_no_anomalies",
    metric_name="row_count",
    input_rows=os.path.join(BASE_DIR, "row_count_1_day_1_row_test.csv"),
    input_rows_history=os.path.join(
        BASE_DIR, "row_count_1_day_1_row_history.csv"
    ),
    expected_output=os.path.join(
        BASE_DIR, "row_count_1_day_1_row_after.csv"
    ),
    alias_table_name="any_type_column_anomalies_training",
)
@Parametrization.case(
    name="row_count_1_day_mostly_non_anomalous",
    metric_name="row_count",
    input_rows=os.path.join(BASE_DIR, "row_count_1_day_test.csv"),
    input_rows_history=os.path.join(
        BASE_DIR, "row_count_1_day_history.csv"
    ),
    expected_output=os.path.join(
        BASE_DIR, "row_count_1_day_after.csv"
    ),
    alias_table_name="string_column_anomalies_training",
)
@Parametrization.case(
    name="row_count_4_hour_non_empty_history_with_anomalies_in_the_end",
    metric_name="row_count",
    input_rows=os.path.join(
        BASE_DIR,
        "row_count_4_hour_test.csv",
    ),
    input_rows_history=os.path.join(
        BASE_DIR,
        "row_count_4_hour_history.csv",
    ),
    expected_output=os.path.join(
        BASE_DIR,
        "row_count_4_hour_after.csv",
    ),
    alias_table_name="any_type_column_anomalies",
    time_bucket={"period": "hour", "count": 4},
)
def test_anomaly_scores_query(
    dbt_project: DbtProject,
    input_rows,
    input_rows_history,
    expected_output,
    metric_name,
    alias_table_name,
    time_bucket,
    timestamp_column,
    days_back,
    where_expression=None,
    dimensions=None,
    columns_only=False,
    column_name=None,
):
    """
    get_anomaly_scores_query macro returns a query that expects 2 input tables.
    first - history of metrics, assumed to be the parameter `data_monitoring_metrics_table` if given, or else,
    `<db>.<schema>_elementary.data_monitoring_metrics` .
    second - newly calculated metrics, assumed to be in the paramaeter `test_metrics_table_relation` .

    The macro queries the historic rows, where relevant to the metric_properties the current test is testing , and also
        where relevant to the current `column_name` if given, as well as the `dimensions` if given.
      Then it joins with the newly calculated metrics, dedups based on metric_id, bucket start, and a few more. Then,
      it groups by metric_id, table name and a few other things, and calculates the average, STD, and the z-score.
    :param days_back:
    :param dbt_project:
    :param input_rows: Either path for csv with input rows, or as a list of dicts
    :param input_rows_history: Either path for csv with historical rows, or as a list of dicts
    :param expected_output: Either path for csv with expected output, or as a list of dicts
    :param time_bucket:
    :param timestamp_column:
    :param where_expression:
    :return:
    """
    update_var(
        dbt_project,
        "custom_run_started_at",
        RUN_STARTED_AT.strftime("%Y-%m-%d %H:%M:%S"),
    )

    my_test_metrics_relation = create_test_table(
        dbt_project,
        "my_test_metrics_table_relation",
        {x: y for x, y in DATA_MONITORING_METRICS_COLUMNS_TO_TYPES},
    )
    my_test_data_monitoring_metrics = create_test_table(
        dbt_project,
        "my_test_data_monitoring_metrics",
        {x: y for x, y in DATA_MONITORING_METRICS_COLUMNS_TO_TYPES},
    )

    insert_rows(dbt_project, my_test_metrics_relation, input_rows)
    insert_rows(dbt_project, my_test_data_monitoring_metrics, input_rows_history)

    metric_properties = {
        "time_bucket": time_bucket,
        "timestamp_column": timestamp_column,
        "where_expression": where_expression,
        "freshness_column": None,
        "event_timestamp_column": None
    }
    if dbt_project.adapter_name not in ["postgres", "redshift"]:
        database, schema = get_package_database_and_schema(dbt_project)
    else:
        database = schema = None

    node = {
        "resource_type": "dummy_type_not_incremental",
        "alias": alias_table_name,
        "database": database,
        "schema": schema,
    }

    query = dbt_project.execute_macro(
        "elementary.get_anomaly_scores_query",
        test_metrics_table_relation=my_test_metrics_relation,
        model_graph_node=node,  # the table the test has ran on / is monitoring.
        sensitivity=3,  # for z-score (3 sigmas is the default)
        backfill_days=2,  # should be removed from the macro since it's not used TODO
        days_back=30,
        monitors=[
            metric_name
        ],  # a list of strings that should include "metric_name" as well.
        # in code it's used to calc a bunch of metrics "all at once" on 1
        # table, but in tests I think it makes sense to have 1 at a time.
        column_name=column_name,  # should be the same as in the input
        columns_only=columns_only,  # if True, don't do table-level anomalies
        dimensions=dimensions,  # should be the same as in the input
        metric_properties=metric_properties,
        data_monitoring_metrics_table=my_test_data_monitoring_metrics,
    )
    res_table = dbt_project.execute_sql(query)
    res_table = lowercase_column_names(res_table)
    res_df = agate_table_to_pandas_dataframe(res_table)
    if isinstance(expected_output, list):
        expected_res_df = pd.DataFrame(expected_output)
    else:
        expected_res_df = pd.read_csv(expected_output)
    expected_res_df = expected_res_df.fillna("")
    res_df = res_df.fillna("")
    assert_dfs_equal(
        expected_res_df,
        res_df,
        columns_to_ignore=["detected_at", "id", "test_execution_id", "test_unique_id"],
        column_to_index_by="bucket_start",
        datetime_columns=[
            "bucket_start",
            "bucket_end",
            "detected_at",
            "training_start",
            "training_end",
        ],
        numeric_columns=[
            "anomaly_score",
            "anomaly_score_threshold",
            "metric_value",
            "min_metric_value",
            "max_metric_value",
            "training_avg",
            "training_stddev",
            "training_set_size",
        ],
    )
