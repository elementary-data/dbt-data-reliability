from datetime import datetime

from parametrization import Parametrization


from .dbt_project import DbtProject
from .utils import create_test_table, insert_rows, update_var, lowercase_column_names


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
    query = dbt_project.execute_macro(
        "elementary.table_monitoring_query",
        monitored_table_relation=relation,
        min_bucket_start=MIN_BUCKET_START.strftime("'%Y-%m-%d %H:%M:%S'"),
        table_monitors=[metric],
        metric_properties=metric_properties,
    )

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
