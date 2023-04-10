from datetime import datetime

from parametrization import Parametrization


from .dbt_project import DbtProject
from .utils import create_test_table, insert_rows, update_var, lowercase_column_names, get_package_database_and_schema

MIN_BUCKET_START = datetime(2022, 1, 1, 0, 0, 0)
RUN_STARTED_AT = datetime(2022, 1, 4, 0, 13, 42)


@Parametrization.autodetect_parameters()
@Parametrization.default_parameters(
    time_bucket={"period": "day", "count": 1},
    timestamp_column="updated_at",
    metric_args={},
)
@Parametrization.case(
    name="row_count",
    metric="row_count",
    input_rows=[
        {"name": "Jessica Jones", "updated_at": "2022-01-01 13:34:29"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 13:35:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 15:23:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 17:48:28"},
    ],
    expected_metrics={
        datetime(2022, 1, 2, 0, 0, 0): 1,
        datetime(2022, 1, 3, 0, 0, 0): 3,
        datetime(2022, 1, 4, 0, 0, 0): 0,
    },
)
@Parametrization.case(
    name="row_count_no_timestamp",
    metric="row_count",
    timestamp_column=None,
    input_rows=[
        {"name": "Jessica Jones", "updated_at": "2022-01-01 13:34:29"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 13:35:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 15:23:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 17:48:28"},
    ],
    expected_metrics={RUN_STARTED_AT: 4},
)
@Parametrization.case(
    name="freshness",
    metric="freshness",
    input_rows=[
        {"name": "Scarlet Witch", "updated_at": "2022-01-01 8:00:00"},
        {"name": "Dr Strange", "updated_at": "2022-01-01 17:00:00"},
        {"name": "Spiderman", "updated_at": "2022-01-02 2:00:00"},
        {"name": "Ironman", "updated_at": "2022-01-02 11:00:00"},
        {"name": "Hulk", "updated_at": "2022-01-02 20:00:00"},
        {"name": "Thor", "updated_at": "2022-01-03 5:00:00"},
    ],
    expected_metrics={
        datetime(2022, 1, 2, 0, 0, 0): 32400,
        datetime(2022, 1, 3, 0, 0, 0): 32400,
        datetime(2022, 1, 4, 0, 0, 0): 68400,
    },
)
@Parametrization.case(
    name="event_freshness_both_timestamps",
    metric="event_freshness",
    input_rows=[
        {
            "name": "Scarlet Witch",
            "updated_at": "2022-01-01 8:00:00",
            "occurred_at": "2022-01-01 7:00:00",
        },
        {
            "name": "Dr Strange",
            "updated_at": "2022-01-01 17:00:00",
            "occurred_at": "2022-01-01 15:30:00",
        },
        {
            "name": "Spiderman",
            "updated_at": "2022-01-02 2:00:00",
            "occurred_at": "2022-01-01 23:59:00",
        },
        {
            "name": "Ironman",
            "updated_at": "2022-01-02 11:00:00",
            "occurred_at": "2022-01-02 10:50:00",
        },
        {
            "name": "Hulk",
            "updated_at": "2022-01-02 20:00:00",
            "occurred_at": "2022-01-02 19:20:00",
        },
    ],
    expected_metrics={
        datetime(2022, 1, 2, 0, 0, 0): 5400,
        datetime(2022, 1, 3, 0, 0, 0): 7260,
        datetime(2022, 1, 4, 0, 0, 0): 86400,
    },
    metric_args={"event_timestamp_column": "occurred_at"},
)
@Parametrization.case(
    name="event_freshness_event_timestamp_only",
    metric="event_freshness",
    timestamp_column=None,
    input_rows=[
        {"name": "Scarlet Witch", "occurred_at": "2022-01-01 7:00:00"},
        {"name": "Dr Strange", "occurred_at": "2022-01-01 15:30:00"},
        {"name": "Spiderman", "occurred_at": "2022-01-01 23:59:00"},
        {"name": "Ironman", "occurred_at": "2022-01-02 10:50:00"},
        {"name": "Hulk", "occurred_at": "2022-01-02 19:20:00"},
    ],
    expected_metrics={RUN_STARTED_AT: 104022},
    metric_args={"event_timestamp_column": "occurred_at"},
)
@Parametrization.case(
    name="row_count_custom_time_bucket",
    metric="row_count",
    time_bucket={"period": "hour", "count": 8},
    input_rows=[
        {"name": "Jessica Jones", "updated_at": "2022-01-01 13:34:29"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 13:35:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 15:23:28"},
        {"name": "Luke Cage", "updated_at": "2022-01-02 17:48:28"},
    ],
    expected_metrics={
        datetime(2022, 1, 1, 8, 0, 0): 0,
        datetime(2022, 1, 1, 16, 0, 0): 1,
        datetime(2022, 1, 2, 0, 0, 0): 0,
        datetime(2022, 1, 2, 8, 0, 0): 0,
        datetime(2022, 1, 2, 16, 0, 0): 2,
        datetime(2022, 1, 3, 0, 0, 0): 1,
        datetime(2022, 1, 3, 8, 0, 0): 0,
        datetime(2022, 1, 3, 16, 0, 0): 0,
        datetime(2022, 1, 4, 0, 0, 0): 0,
    },
)
def test_table_monitoring_query(
    dbt_project: DbtProject,
    metric,
    input_rows,
    expected_metrics,
    time_bucket,
    timestamp_column,
    metric_args,
):
    update_var(
        dbt_project,
        "custom_run_started_at",
        RUN_STARTED_AT.strftime("%Y-%m-%d %H:%M:%S"),
    )
    relation = create_test_table(
        dbt_project,
        "my_test_table",
        {"name": "string", "updated_at": "timestamp", "occurred_at": "timestamp"},
    )
    insert_rows(dbt_project, relation, input_rows)
    metric_properties = {
        "time_bucket": time_bucket,
        "timestamp_column": timestamp_column,
        "where_expression": None,
        # dict.get(x) defaults to dict.get(x, None) so this
        "freshness_column": metric_args.get("freshness_column"),
        "event_timestamp_column": metric_args.get("event_timestamp_column"),
    }
    # macro get_test_buckets_min_and_max(model_relation, backfill_days, days_back, monitors=none, column_name=none, metric_properties=none, unit_test=false, unit_test_relation=none)
    if dbt_project.adapter_name not in ["postgres", "redshift"]:
        database, schema = get_package_database_and_schema(dbt_project)
    else:
        database = schema = None

    model_relation = {
        "identifier": relation.identifier,
        "database": database,
        "schema": schema,
    }

    monitors_runs_schema = dict([('full_table_name', 'string'), ('metric_properties', 'string'), ('metric_name', 'string') ,('last_bucket_end', 'timestamp'), ('first_bucket_end', 'timestamp')])
    monitors_runs_relation=create_test_table(dbt_project,'monitors_runs_unit_test', monitors_runs_schema)

    buckets_start_and_end = dbt_project.execute_macro(
        "elementary.get_test_buckets_min_and_max",
        model_relation=model_relation,
        backfill_days=2,
        days_back=3,
        monitors=[metric],
        column_name=None,
        metric_properties=metric_properties,
        unit_test=True,
        unit_test_relation=monitors_runs_relation,
    )

    query = dbt_project.execute_macro(
        "elementary.table_monitoring_query",
        monitored_table_relation=relation,
        min_bucket_start=buckets_start_and_end[0],
        max_bucket_end=buckets_start_and_end[1],
        table_monitors=[metric],
        days_back=3,
        metric_properties=metric_properties,
    )
    #import pdb;pdb.set_trace() # NO_COMMIT
    res_table = dbt_project.execute_sql(query)

    res_table = lowercase_column_names(res_table)
    assert len(res_table) == len(expected_metrics)  # Ensure there are no duplicates

    result_metrics = {
        row["bucket_end"].replace(tzinfo=None): row["metric_value"] for row in res_table
    }
    assert result_metrics == expected_metrics


# @dataclass
# class Node:
#     database: str
#     schema: str
#     identifier: str
#     alias: Optional[str]
#     name: Optional[str]
#
