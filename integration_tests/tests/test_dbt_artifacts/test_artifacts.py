from datetime import datetime

import pytest
from dbt_flags import set_flags
from dbt_project import DbtProject
from dbt_utils import get_database_and_schema_properties

TEST_MODEL = "one"


def read_model_artifact_row(dbt_project: DbtProject):
    return dbt_project.read_table("dbt_models", where=f"alias = '{TEST_MODEL}'")[0]


@pytest.mark.requires_dbt_version("1.4.0")
def test_artifacts_caching(dbt_project: DbtProject):
    # Disabled by default in the tests for performance reasons.
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False

    dbt_project.dbt_runner.vars["cache_artifacts"] = True

    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["hello", "world"]})
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_tags": ["world", "hello"]})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row == second_row, "Artifacts are not cached at the on-run-end."


@pytest.mark.skip_targets(["dremio"])
def test_artifacts_updating(dbt_project: DbtProject):
    # Disabled by default in the tests for performance reasons.
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False

    dbt_project.dbt_runner.vars["cache_artifacts"] = True

    dbt_project.dbt_runner.run(select=TEST_MODEL)
    first_row = read_model_artifact_row(dbt_project)
    dbt_project.dbt_runner.run(select=TEST_MODEL, vars={"one_owner": "ele"})
    second_row = read_model_artifact_row(dbt_project)
    assert first_row != second_row, "Artifacts are not updated at the on-run-end."


def test_artifacts_collection_in_multiple_row_batches(dbt_project: DbtProject):
    existing_artifacts = dbt_project.read_table("dbt_models")

    dbt_project.dbt_runner.vars[
        "query_max_size"
    ] = 5000  # small value to force multiple batches
    dbt_project.dbt_runner.vars[
        "cache_artifacts"
    ] = False  # force artifacts to be recollected

    dbt_project.dbt_runner.run(select="dbt_models")

    new_artifacts = dbt_project.read_table("dbt_models")

    assert len(existing_artifacts) == len(new_artifacts)


def test_replace_table_data(dbt_project: DbtProject):
    """Validate that replace_table_data actually replaces (not diffs) data.

    Sets cache_artifacts=False so the upload path uses replace_table_data.
    Inserts an unrelated sentinel row into dbt_models *before* the replace
    run, then asserts it was removed — proving a full table replace happened
    rather than a diff-based update (which would leave unrelated rows intact).
    """
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
    dbt_project.dbt_runner.vars["cache_artifacts"] = False

    SENTINEL_ALIAS = "__replace_test_sentinel__"

    # Populate the table with real artifacts first.
    dbt_project.dbt_runner.run(select=TEST_MODEL)

    # Inject a sentinel row that no real dbt model would produce.
    # Uses a dbt macro (run_operation) so the INSERT is committed properly
    # across all adapters.
    dbt_project.dbt_runner.run_operation(
        "elementary_tests.insert_sentinel_row",
        macro_args={"table_name": "dbt_models", "sentinel_alias": SENTINEL_ALIAS},
    )
    sentinel_rows = dbt_project.read_table(
        "dbt_models", where=f"alias = '{SENTINEL_ALIAS}'", raise_if_empty=False
    )
    assert len(sentinel_rows) == 1, "Sentinel row was not inserted"

    # Run again with cache_artifacts=False → triggers replace_table_data.
    dbt_project.dbt_runner.run(select=TEST_MODEL)

    # The sentinel must be gone — replace_table_data wipes the whole table.
    sentinel_after = dbt_project.read_table(
        "dbt_models", where=f"alias = '{SENTINEL_ALIAS}'", raise_if_empty=False
    )
    assert len(sentinel_after) == 0, (
        "replace_table_data did not remove unrelated rows — "
        "sentinel row still present (diff mode would keep it, replace should not)"
    )

    # The real model row must still exist.
    real_row = read_model_artifact_row(dbt_project)
    assert real_row["alias"] == TEST_MODEL


def test_dbt_invocations(dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_dbt_invocation_autoupload"] = False
    dbt_project.dbt_runner.run(selector="one")
    dbt_project.read_table(
        "dbt_invocations", where="yaml_selector = 'one'", raise_if_empty=True
    )


def test_seed_run_results(dbt_project: DbtProject):
    dbt_project.read_table("seed_run_results", raise_if_empty=False)


def test_job_run_results(dbt_project: DbtProject):
    dbt_project.read_table("job_run_results", raise_if_empty=False)


def test_model_run_results(dbt_project: DbtProject):
    dbt_project.read_table("model_run_results", raise_if_empty=False)


def test_snapshot_run_results(dbt_project: DbtProject):
    dbt_project.read_table("snapshot_run_results", raise_if_empty=False)


def test_monitors_runs(dbt_project: DbtProject):
    dbt_project.read_table("monitors_runs", raise_if_empty=False)


def test_dbt_artifacts_hashes(dbt_project: DbtProject):
    dbt_project.read_table("dbt_artifacts_hashes", raise_if_empty=False)


def test_anomaly_threshold_sensitivity(dbt_project: DbtProject):
    dbt_project.read_table("anomaly_threshold_sensitivity", raise_if_empty=False)


def test_metrics_anomaly_score(dbt_project: DbtProject):
    dbt_project.read_table("metrics_anomaly_score", raise_if_empty=False)


@pytest.mark.requires_dbt_version("1.8.0")
@pytest.mark.skip_for_dbt_fusion
def test_source_freshness_results(test_id: str, dbt_project: DbtProject):
    database_property, schema_property = get_database_and_schema_properties(
        dbt_project.target
    )
    loaded_at_field = (
        '"UPDATE_TIME"::timestamp'
        if dbt_project.target != "dremio"
        else "TO_TIMESTAMP(SUBSTRING(UPDATE_TIME, 0, 23), 'YYYY-MM-DD HH24:MI:SS.FFF')"
    )
    source_def = {
        "name": "test_source",
        "schema": f"{{{{ target.{schema_property} }}}}",
        "tables": [
            {
                "name": test_id,
                "config": {
                    "loaded_at_field": loaded_at_field,
                    "freshness": {
                        "warn_after": {
                            "count": 1,
                            "period": "hour",
                        },
                    },
                },
            }
        ],
    }
    if database_property is not None:
        source_def["database"] = f"{{{{ target.{database_property} }}}}"
    source_config = {
        "version": 2,
        "sources": [source_def],
    }
    dbt_project.seed(
        [
            {
                "UPDATE_TIME": datetime.now(),
            }
        ],
        test_id,
    )

    dbt_project.dbt_runner.vars["disable_freshness_results"] = False
    with dbt_project.write_yaml(content=source_config), set_flags(
        dbt_project, {"source_freshness_run_project_hooks": True}
    ):
        dbt_project.dbt_runner.source_freshness()
        dbt_project.read_table(
            "dbt_source_freshness_results",
            where=f"unique_id = 'source.elementary_tests.test_source.{test_id}'",
            raise_if_empty=True,
        )


def test_timings(dbt_project: DbtProject):
    dbt_project.dbt_runner.vars["disable_dbt_artifacts_autoupload"] = False
    dbt_project.dbt_runner.vars["disable_run_results"] = False
    dbt_project.dbt_runner.run(select=TEST_MODEL)
    results = dbt_project.run_query(
        """select * from {{ ref("dbt_run_results") }} where name='%s'""" % TEST_MODEL
    )

    assert len(results) == 1
    assert results[0]["execute_started_at"]


@pytest.mark.only_on_targets(["bigquery"])
def test_run_results_partitioned(dbt_project: DbtProject):
    # BigQuery partitioning is enabled by default. Verify the model works and data is readable.
    dbt_project.dbt_runner.vars["disable_run_results"] = False
    # Full-refresh to ensure the table is created with partitioning
    dbt_project.dbt_runner.run(select="dbt_run_results", full_refresh=True)
    dbt_project.dbt_runner.run(select=TEST_MODEL)
    results = dbt_project.run_query(
        """select * from {{ ref("dbt_run_results") }} where name='%s'""" % TEST_MODEL
    )
    assert len(results) >= 1

    # Verify the partition column is created_at in BigQuery (uses get_partition_by default)
    partition_cols = dbt_project.run_query(
        "SELECT column_name "
        "FROM `{{ ref('dbt_run_results').database }}.{{ ref('dbt_run_results').schema }}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = '{{ ref('dbt_run_results').identifier }}' "
        "AND is_partitioning_column = 'YES'"
    )
    assert [row["column_name"] for row in partition_cols] == [
        "created_at"
    ], "dbt_run_results should be partitioned by created_at in BigQuery"


@pytest.mark.only_on_targets(["bigquery"])
def test_dbt_invocations_partitioned(dbt_project: DbtProject):
    # BigQuery partitioning is enabled by default. Verify dbt_invocations works.
    dbt_project.dbt_runner.vars["disable_dbt_invocation_autoupload"] = False
    # Full-refresh to ensure the table is created with partitioning
    dbt_project.dbt_runner.run(select="dbt_invocations", full_refresh=True)
    dbt_project.dbt_runner.run(selector="one")
    dbt_project.read_table(
        "dbt_invocations", where="yaml_selector = 'one'", raise_if_empty=True
    )

    # Verify the partition column is created_at in BigQuery
    partition_cols = dbt_project.run_query(
        "SELECT column_name "
        "FROM `{{ ref('dbt_invocations').database }}.{{ ref('dbt_invocations').schema }}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = '{{ ref('dbt_invocations').identifier }}' "
        "AND is_partitioning_column = 'YES'"
    )
    assert [row["column_name"] for row in partition_cols] == [
        "created_at"
    ], "dbt_invocations should be partitioned by created_at in BigQuery"


@pytest.mark.only_on_targets(["bigquery"])
def test_data_monitoring_metrics_partitioned(dbt_project: DbtProject):
    # data_monitoring_metrics is partitioned by bucket_end on BigQuery.
    # Full-refresh to ensure the table is created with partitioning.
    dbt_project.dbt_runner.run(select="data_monitoring_metrics", full_refresh=True)

    partition_cols = dbt_project.run_query(
        "SELECT column_name "
        "FROM `{{ ref('data_monitoring_metrics').database }}.{{ ref('data_monitoring_metrics').schema }}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = '{{ ref('data_monitoring_metrics').identifier }}' "
        "AND is_partitioning_column = 'YES'"
    )
    assert [row["column_name"] for row in partition_cols] == [
        "bucket_end"
    ], "data_monitoring_metrics should be partitioned by bucket_end in BigQuery"


@pytest.mark.only_on_targets(["bigquery"])
def test_data_monitoring_metrics_clustered(dbt_project: DbtProject):
    # data_monitoring_metrics is clustered by full_table_name and metric_name on BigQuery.
    # Full-refresh to ensure the table is created with clustering.
    dbt_project.dbt_runner.run(select="data_monitoring_metrics", full_refresh=True)

    clustering_cols = dbt_project.run_query(
        "SELECT column_name "
        "FROM `{{ ref('data_monitoring_metrics').database }}.{{ ref('data_monitoring_metrics').schema }}.INFORMATION_SCHEMA.COLUMNS` "
        "WHERE table_name = '{{ ref('data_monitoring_metrics').identifier }}' "
        "AND clustering_ordinal_position IS NOT NULL "
        "ORDER BY clustering_ordinal_position"
    )
    assert [row["column_name"] for row in clustering_cols] == [
        "full_table_name",
        "metric_name",
    ], "data_monitoring_metrics should be clustered by full_table_name, metric_name in BigQuery"
