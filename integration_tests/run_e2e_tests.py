import os
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import List

import click

from elementary.clients.dbt.dbt_runner import DbtRunner
from generate_data import generate_fake_data

FILE_DIR = os.path.dirname(os.path.realpath(__file__))

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

    def extend(self, test_results: List[TestResult]):
        if not test_results:
            raise ValueError("Received an empty test results list.")
        for test_result in test_results:
            print(test_result)
        self.results.extend(test_results)

    def get_failed(self):
        return [result for result in self.results if not result.success]


class TestDbtRunner(DbtRunner):
    def upload_source_freshness(self):
        self.run_operation("elementary.upload_source_freshness")


def e2e_tests(
    target: str, test_types: List[str], clear_tests: bool, generate_data: bool
) -> TestResults:
    test_results = TestResults()

    dbt_runner = TestDbtRunner(
        project_dir=FILE_DIR,
        target=target,
        raise_on_failure=False,
    )

    if generate_data:
        dbt_runner.seed(full_refresh=True)

    if clear_tests:
        clear_test_logs = dbt_runner.run_operation(macro_name="clear_tests")
        for clear_test_log in clear_test_logs:
            print(clear_test_log)

    dbt_runner.run(vars={"stage": "training"})

    if "table" in test_types:
        dbt_runner.test(select="tag:table_anomalies")
        results = [
            TestResult(type="table_anomalies", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_table_anomalies")
        ]
        test_results.extend(results)

    if "create_table" in test_types:
        # If there is a problem with create_or_replace macro, it will crash the test.
        dbt_runner.test(select="tag:table_anomalies")
        dbt_runner.test(select="tag:table_anomalies")

    if "error_test" in test_types:
        dbt_runner.test(select="tag:error_test")
        results = [
            TestResult(type="error_test", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_error_test")
        ]
        test_results.extend(results)

    if "error_model" in test_types:
        dbt_runner.run(select="tag:error_model")
        results = [
            TestResult(type="error_model", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_error_model")
        ]
        test_results.extend(results)

    if "error_snapshot" in test_types:
        dbt_runner.snapshot()
        results = [
            TestResult(type="error_snapshot", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_error_snapshot")
        ]
        test_results.extend(results)

    # Creates row_count metrics for anomalies detection.
    if "no_timestamp" in test_types:
        # Run operation returns the operation value as a list of strings.
        # So we convert the days_back value into int.
        days_back_project_var = int(
            dbt_runner.run_operation(
                macro_name="return_config_var", macro_args={"var_name": "days_back"}
            )[0]
        )
        # No need to create today's metric because the validation run does it.
        for run_index in range(1, days_back_project_var):
            custom_run_time = (
                EPOCH - timedelta(days_back_project_var - run_index)
            ).isoformat()
            dbt_runner.test(
                select="tag:no_timestamp",
                vars={"custom_run_started_at": custom_run_time},
            )

    if "schema" in test_types and target not in ["databricks", "spark"]:
        dbt_runner.test(select="tag:schema_changes")
        dbt_runner.test(select="tag:schema_changes_from_baseline")

    dbt_runner.run(vars={"stage": "validation"})

    if "debug" in test_types:
        dbt_runner.test(select="tag:debug")
        return test_results

    if "no_timestamp" in test_types:
        dbt_runner.test(select="tag:no_timestamp")
        results = [
            TestResult(type="no_timestamp_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="validate_no_timestamp_anomalies"
            )
        ]
        test_results.extend(results)

    if "column" in test_types:
        dbt_runner.test(select="tag:string_column_anomalies")
        results = [
            TestResult(type="string_column_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="validate_string_column_anomalies"
            )
        ]
        test_results.extend(results)

        dbt_runner.test(select="tag:numeric_column_anomalies")
        results = [
            TestResult(type="numeric_column_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="validate_numeric_column_anomalies"
            )
        ]
        test_results.extend(results)

        dbt_runner.test(select="tag:all_any_type_columns_anomalies")
        results = [
            TestResult(type="any_type_column_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="validate_any_type_column_anomalies"
            )
        ]
        test_results.extend(results)

    if "dimension" in test_types:
        dbt_runner.test(select="tag:dimension_anomalies")
        results = [
            TestResult(type="dimension_anomalies", message=msg)
            for msg in dbt_runner.run_operation(
                macro_name="validate_dimension_anomalies"
            )
        ]
        test_results.extend(results)

    if "schema" in test_types and target not in ["databricks", "spark"]:
        dbt_runner.test(select="tag:schema_changes")
        results = [
            TestResult(type="schema_changes", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_schema_changes")
        ]
        test_results.extend(results)

    if "regular" in test_types:
        dbt_runner.test(select="test_type:singular tag:regular_tests")
        results = [
            TestResult(type="regular_tests", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_regular_tests")
        ]
        test_results.extend(results)

    if "artifacts" in test_types:
        results = [
            TestResult(type="artifacts", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_dbt_artifacts")
        ]
        test_results.extend(results)

    if "source_freshness" in test_types:
        dbt_runner.source_freshness()
        dbt_runner.upload_source_freshness()
        results = [
            TestResult(type="source_freshness", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_source_freshness")
        ]
        test_results.extend(results)

    return test_results


def print_failed_test_results(e2e_target: str, failed_test_results: List[TestResult]):
    print(f"Failed {e2e_target} tests:")
    for failed_test_result in failed_test_results:
        print(f"{failed_test_result.type}: {failed_test_result.message}")


@click.command()
@click.option(
    "--target",
    "-t",
    type=str,
    default="all",
    help="snowflake / bigquery / redshift / all (default = all)",
)
@click.option(
    "--e2e-type",
    "-e",
    type=str,
    default="all",
    help="table / column / schema / regular / artifacts / error_test / error_model / error_snapshot / dimension / create_table / no_timestamp / debug / all (default = all)",
)
@click.option(
    "--generate-data",
    "-g",
    type=bool,
    default=False,
    help="Set to true if you want to re-generate fake data (default = True)",
)
@click.option(
    "--clear-tests",
    type=bool,
    default=True,
    help="Set to true if you want to clear the tests (default = True)",
)
def main(target, e2e_type, generate_data, clear_tests):
    if generate_data:
        generate_fake_data()

    if target == "all":
        e2e_targets = ["snowflake", "bigquery", "redshift"]
    else:
        e2e_targets = [target]

    if e2e_type == "all":
        e2e_types = [
            "table",
            "column",
            "schema",
            "regular",
            "artifacts",
            "error_test",
            "error_model",
            "error_snapshot",
            "dimension",
            "create_table",
            "source_freshness",
        ]
    else:
        e2e_types = [e2e_type]

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
