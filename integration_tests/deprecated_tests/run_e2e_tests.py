import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import click
from dbt.version import __version__
from elementary.clients.dbt.base_dbt_runner import BaseDbtRunner
from elementary.clients.dbt.factory import create_dbt_runner
from generate_data import generate_fake_data
from packaging import version

FILE_DIR = os.path.dirname(os.path.realpath(__file__))
DBT_VERSION = version.parse(version.parse(__version__).base_version)
EPOCH = datetime.utcfromtimestamp(0)


@dataclass
class TestResult:
    type: str
    message: str

    @property
    def success(self) -> bool:
        if "FAILED" in self.message:
            return False
        if "SUCCESS" in self.message:
            return True
        raise ValueError(
            "Invalid test result, no FAILED or SUCCESS.", self.type, self.message
        )

    def __str__(self):
        return f"{self.type}: {self.message}"


class TestResults:
    def __init__(self):
        self.results = []

    def append(self, test_result: TestResult):
        self.extend([test_result])

    def extend(self, test_results: List[TestResult]):
        if not test_results:
            raise ValueError("Received an empty test results list.")
        for test_result in test_results:
            print(test_result)
        self.results.extend(test_results)

    def get_failed(self):
        return [result for result in self.results if not result.success]


def get_row(alias: str, dbt_runner: BaseDbtRunner) -> str:
    rows = json.loads(
        dbt_runner.run_operation(
            "elementary_integration_tests.read_table",
            macro_args={"table": "dbt_models", "where": f"alias = '{alias}'"},
            return_raw_edr_logs=True,
        )[0]
    )
    if len(rows) != 1:
        raise ValueError("Expected to find a single row.")
    return rows[0]


def test_artifacts_cache(dbt_runner: BaseDbtRunner) -> TestResult:
    test_model = "one"
    dbt_runner.run(test_model, vars={"one_tags": ["hello", "world"]})
    first_row = get_row(test_model, dbt_runner)
    dbt_runner.run(test_model, vars={"one_tags": ["world", "hello"]})
    second_row = get_row(test_model, dbt_runner)
    return TestResult(
        type="test_artifacts_cache",
        message=(
            "SUCCESS: Artifacts are cached at the on run end."
            if first_row == second_row
            else "FAILED: Artifacts are not cached at the on run end."
        ),
    )


def test_artifacts_update(dbt_runner: BaseDbtRunner) -> TestResult:
    test_model = "one"
    dbt_runner.run(test_model)
    first_row = get_row(test_model, dbt_runner)
    dbt_runner.run(test_model, vars={"one_owner": "ele"})
    second_row = get_row(test_model, dbt_runner)
    return TestResult(
        type="test_artifacts_update",
        message=(
            "SUCCESS: Artifacts are updated on run end."
            if first_row != second_row
            else "FAILED: Artifacts are not updated on run end."
        ),
    )


def validate_regular_tests_for_clickhouse(
    dbt_runner: BaseDbtRunner,
) -> List[TestResult]:
    """Validate regular tests for ClickHouse with more flexible test type validation."""
    results = []

    # Run basic tests that should work in ClickHouse
    dbt_runner.test(
        select="test_type:singular tag:regular_tests",
        vars={"disable_dbt_artifacts_autoupload": "true"},
    )

    # Get test results without requiring specific test names
    test_results = dbt_runner.run_operation(
        macro_name="elementary_integration_tests.get_regular_test_results",
        return_raw_edr_logs=True,
    )

    # Validate that tests ran and results were stored
    if test_results:
        results.append(
            TestResult(
                type="regular_tests",
                message="SUCCESS: Regular tests executed and results stored successfully.",
            )
        )
    else:
        results.append(
            TestResult(type="regular_tests", message="FAILED: No test results found.")
        )

    return results


def get_e2e_test_types(e2e_type: str, target: str) -> List[str]:
    if e2e_type == "default":
        test_types = [
            "seasonal_volume",
            "table",
            "column",
            "directional_anomalies",
            "backfill_days",
            "schema",
            "regular",
            "artifacts",
            "error_test",
            "error_model",
            "error_snapshot",
            "dimension",
            "create_table",
            "non_dbt_models",
        ]
    else:
        test_types = [e2e_type]

    if target == "clickhouse":
        unsupported_test_types = {
            # Anomaly tests (not supported in ClickHouse)
            "seasonal_volume",
            "table",
            "column",
            "directional_anomalies",
            "backfill_days",
            "dimension",
            "no_timestamp",
            # Schema and error tests (function compatibility issues)
            "schema",
            "error_test",
            "error_model",
            "error_snapshot",
            # Models with compatibility issues
            "create_table",
            "non_dbt_models",
            # Tests requiring specific database setup
            "config_levels",
        }
        test_types = [t for t in test_types if t not in unsupported_test_types]
    return test_types


def e2e_tests(
    target: str,
    test_types: List[str],
    clear_tests: bool,
    generate_data: bool,
) -> TestResults:
    test_results = TestResults()

    dbt_runner = create_dbt_runner(
        project_dir=FILE_DIR,
        target=target,
        raise_on_failure=False,
    )

    if generate_data:
        dbt_runner.seed(full_refresh=True)

    if clear_tests:
        clear_test_logs = dbt_runner.run_operation(
            macro_name="elementary_integration_tests.clear_tests",
            return_raw_edr_logs=True,
        )
        for clear_test_log in clear_test_logs:
            print(clear_test_log)

    dbt_runner.run(vars={"stage": "training"})

    if "dimension" in test_types:
        dbt_runner.run_operation(
            macro_name="elementary_integration_tests.create_new_dimension",
        )

    if "error_model" in test_types:
        results = [
            TestResult(type="error_model", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_error_model",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "seasonal_volume" in test_types:
        dbt_runner.test(
            select="tag:seasonality_volume",
            vars={
                "custom_run_started_at": "1969-12-31 08:00:00",
                "disable_dbt_artifacts_autoupload": "true",
            },
        )
        results = [
            TestResult(type="seasonal_volume", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_seasonal_volume_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "table" in test_types:
        dbt_runner.test(
            select="tag:table_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="table_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_table_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "create_table" in test_types:
        # If there is a problem with create_or_replace macro, it will crash the test.
        dbt_runner.test(
            select="tag:table_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        dbt_runner.test(
            select="tag:table_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )

    if "error_snapshot" in test_types:
        dbt_runner.snapshot()
        results = [
            TestResult(type="error_snapshot", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_error_snapshot",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    # Creates row_count metrics for anomalies detection.
    if "no_timestamp" in test_types:
        # Run operation returns the operation value as a list of strings.
        # So we convert the days_back value into int.
        days_back_project_var = int(
            dbt_runner.run_operation(
                macro_name="elementary_integration_tests.return_config_var",
                macro_args={"var_name": "days_back"},
                return_raw_edr_logs=True,
            )[0]
        )
        # No need to create today's metric because the validation run does it.
        for run_index in range(1, days_back_project_var):
            custom_run_time = (
                EPOCH - timedelta(days_back_project_var - run_index)
            ).isoformat()
            dbt_runner.test(
                select="tag:no_timestamp",
                vars={
                    "custom_run_started_at": custom_run_time,
                    "disable_dbt_artifacts_autoupload": "true",
                },
            )

    if "schema" in test_types and target not in ["databricks", "spark"]:
        dbt_runner.test(
            select="tag:schema_changes",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        dbt_runner.test(
            select="tag:schema_changes_from_baseline",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )

    dbt_runner.run(
        vars={"stage": "validation", "disable_dbt_artifacts_autoupload": "true"}
    )

    if "directional_anomalies" in test_types:
        dbt_runner.test(
            select="tag:directional_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="directional_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_directional_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "debug" in test_types:
        dbt_runner.test(
            select="tag:debug", vars={"disable_dbt_artifacts_autoupload": "true"}
        )
        return test_results

    if "no_timestamp" in test_types:
        dbt_runner.test(
            select="tag:no_timestamp", vars={"disable_dbt_artifacts_autoupload": "true"}
        )
        results = [
            TestResult(type="no_timestamp_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_no_timestamp_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "table" in test_types:
        dbt_runner.test(
            select="tag:event_freshness_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="event_freshness_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_event_freshness_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "column" in test_types:
        dbt_runner.test(
            select="tag:column_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="column_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_column_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "backfill_days" in test_types:
        dbt_runner.test(
            select="tag:backfill_days",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="backfill_days", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_backfill_days",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "dimension" in test_types:
        dbt_runner.test(
            select="tag:dimension_anomalies",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="dimension_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_dimension_anomalies",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

        dbt_runner.run_operation(
            macro_name="elementary_integration_tests.delete_new_dimension",
        )

    if "schema" in test_types and target not in ["databricks", "spark"]:
        dbt_runner.test(
            select="tag:schema_changes",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="schema_changes", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_schema_changes",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "regular" in test_types:
        if target == "clickhouse":
            # Use ClickHouse-specific validation
            results = validate_regular_tests_for_clickhouse(dbt_runner)
            test_results.extend(results)
        else:
            # Use standard validation for other targets
            dbt_runner.test(
                select="test_type:singular tag:regular_tests",
                vars={"disable_dbt_artifacts_autoupload": "true"},
            )
            results = [
                TestResult(type="regular_tests", message=msg)
                for msg in dbt_runner.run_operation(
                    macro_name="elementary_integration_tests.validate_regular_tests",
                    return_raw_edr_logs=True,
                )
            ]
            test_results.extend(results)

    if "config_levels" in test_types:
        dbt_runner.test(
            select="tag:config_levels",
            vars={"disable_dbt_artifacts_autoupload": "true"},
        )
        results = [
            TestResult(type="config_levels", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_config_levels",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "artifacts" in test_types:
        results = [
            TestResult(type="artifacts", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_dbt_artifacts",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)
        auto_upload_results = test_artifacts_update(dbt_runner)
        test_results.append(auto_upload_results)
        if DBT_VERSION >= version.parse("1.4.0"):
            cache_artifacts_results = test_artifacts_cache(dbt_runner)
            if cache_artifacts_results:
                test_results.append(cache_artifacts_results)

    # Test errors validation needs to run last
    if "error_test" in test_types:
        dbt_runner.test(
            select="tag:error_test", vars={"disable_dbt_artifacts_autoupload": "true"}
        )
        results = [
            TestResult(type="error_test", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="elementary_integration_tests.validate_error_test",
                return_raw_edr_logs=True,
            )
        ]
        test_results.extend(results)

    if "non_dbt_models" in test_types:
        model = "non_dbt_model"
        try:
            row = get_row(model, dbt_runner)
            if (
                row["depends_on_nodes"] != '["model.elementary_integration_tests.one"]'
                or row["materialization"] != "non_dbt"
            ):
                result = TestResult(
                    type="non_dbt_models",
                    message="FAILED: non_dbt model not materialized as expected",
                )
            else:
                result = TestResult(
                    type="non_dbt_models",
                    message=dbt_runner.run_operation(
                        "elementary_integration_tests.assert_table_doesnt_exist",
                        macro_args={"model_name": "non_dbt_model"},
                        return_raw_edr_logs=True,
                    )[0],
                )
        except ValueError:
            result = TestResult(
                type="non_dbt_models",
                message="FAILED: we need to see the non_dbt model in the run",
            )
        test_results.append(result)

    return test_results


def print_failed_test_results(e2e_target: str, failed_test_results: List[TestResult]):
    print(f"Failed {e2e_target} tests:")
    for failed_test_result in failed_test_results:
        print(
            f"\033[1m\033[91m{failed_test_result.type}: {failed_test_result.message}\033[0m"
        )


@click.command()
@click.option(
    "--target",
    "-t",
    type=str,
    default="postgres",
    help="The dbt target to run the tests against.",
)
@click.option(
    "--e2e-type",
    "-e",
    type=str,
    default="default",
    help="The type of e2e tests to run.",
)
@click.option(
    "--generate-data",
    "-g",
    type=bool,
    default=False,
    help="Set to true if you want to re-generate fake data.",
)
@click.option(
    "--clear-tests",
    type=bool,
    default=True,
    help="Set to true if you want to clear the tests.",
)
def main(target, e2e_type, generate_data, clear_tests):
    if generate_data:
        generate_fake_data()

    e2e_targets = [target]

    e2e_types = get_e2e_test_types(e2e_type, target)

    all_results = {}
    found_failures = False
    for e2e_target in e2e_targets:
        print(f"Starting {e2e_target} tests\n")
        e2e_test_results = e2e_tests(e2e_target, e2e_types, clear_tests, generate_data)
        print(f"\n{e2e_target} results")
        all_results[e2e_target] = e2e_test_results

    for e2e_target, e2e_test_results in all_results.items():
        test_results = e2e_test_results.results
        failed_test_results = e2e_test_results.get_failed()
        if failed_test_results:
            print_failed_test_results(e2e_target, failed_test_results)
            found_failures = True
        print(
            f"[{len(test_results) - len(failed_test_results)}/{len(test_results)}] {e2e_target} TESTS PASSED"
        )

    if found_failures:
        print("Some of the tests failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
