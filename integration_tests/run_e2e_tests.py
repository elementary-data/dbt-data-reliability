import csv
import os
import random
import string
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta
from os.path import expanduser
from pathlib import Path
from typing import List

import click

from elementary.clients.dbt.dbt_runner import DbtRunner

any_type_columns = ["date", "null_count", "null_percent"]

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def generate_date_range(base_date, numdays=30):
    return [base_date - timedelta(days=x) for x in range(0, numdays)]


def write_rows_to_csv(csv_path, rows, header):
    # Creates the csv file directories if needed.
    directory_path = Path(csv_path).parent.resolve()
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    with open(csv_path, "w") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def generate_rows(rows_count_per_day, dates, get_row_callback):
    rows = []
    for date in dates:
        for i in range(0, rows_count_per_day):
            row = get_row_callback(date, i, rows_count_per_day)
            rows.append(row)
    return rows


def generate_string_anomalies_training_and_validation_files(rows_count_per_day=100):
    def get_training_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 10))
            ),
            "max_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 10))
            ),
            "average_length": "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_count": ""
            if row_index < (3 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_percent": ""
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "min_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(1, 10))
            ),
            "max_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 15))
            ),
            "average_length": "".join(
                random.choices(string.ascii_lowercase, k=random.randint(5, 8))
            ),
            "missing_count": ""
            if row_index < (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "missing_percent": ""
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
        }

    string_columns = [
        "date",
        "min_length",
        "max_length",
        "average_length",
        "missing_count",
        "missing_percent",
    ]
    dates = generate_date_range(
        base_date=datetime.today() - timedelta(days=2), numdays=30
    )
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "string_column_anomalies_training.csv"
        ),
        training_rows,
        string_columns,
    )

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "string_column_anomalies_validation.csv"
        ),
        validation_rows,
        string_columns,
    )


def generate_numeric_anomalies_training_and_validation_files(rows_count_per_day=200):
    def get_training_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "min": random.randint(100, 200),
            "max": random.randint(100, 200),
            "zero_count": 0
            if row_index < (3 / 100 * rows_count)
            else random.randint(100, 200),
            "zero_percent": 0
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.randint(100, 200),
            "average": random.randint(99, 101),
            "standard_deviation": random.randint(99, 101),
            "variance": random.randint(99, 101),
        }

    def get_validation_row(date, row_index, rows_count):
        row_index += -(rows_count / 2)
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "min": random.randint(10, 200),
            "max": random.randint(100, 300),
            "zero_count": 0
            if row_index < (80 / 100 * rows_count)
            else random.randint(100, 200),
            "zero_percent": 0
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.randint(100, 200),
            "average": random.randint(101, 110),
            "standard_deviation": random.randint(80, 120),
            "variance": random.randint(80, 120),
        }

    numeric_columns = [
        "date",
        "min",
        "max",
        "zero_count",
        "zero_percent",
        "average",
        "standard_deviation",
        "variance",
    ]
    dates = generate_date_range(
        base_date=datetime.today() - timedelta(days=2), numdays=30
    )
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "numeric_column_anomalies_training.csv"
        ),
        training_rows,
        numeric_columns,
    )

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "numeric_column_anomalies_validation.csv"
        ),
        validation_rows,
        numeric_columns,
    )


def generate_any_type_anomalies_training_and_validation_files(rows_count_per_day=300):
    def get_training_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "null_count_str": None
            if row_index < (3 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_percent_str": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_count_float": None
            if row_index < (3 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_percent_float": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_count_int": None
            if row_index < (3 / 100 * rows_count)
            else random.randint(100, 200),
            "null_percent_int": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else random.randint(100, 200),
            "null_count_bool": None
            if row_index < (3 / 100 * rows_count)
            else bool(random.getrandbits(1)),
            "null_percent_bool": None
            if random.randint(1, rows_count) <= (20 / 100 * rows_count)
            else bool(random.getrandbits(1)),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "null_count_str": None
            if row_index < (80 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_percent_str": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else "".join(random.choices(string.ascii_lowercase, k=5)),
            "null_count_float": None
            if row_index < (80 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_percent_float": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.uniform(1.2, 8.9),
            "null_count_int": None
            if row_index < (80 / 100 * rows_count)
            else random.randint(100, 200),
            "null_percent_int": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else random.randint(100, 200),
            "null_count_bool": None
            if row_index < (80 / 100 * rows_count)
            else bool(random.getrandbits(1)),
            "null_percent_bool": None
            if random.randint(1, rows_count) <= (60 / 100 * rows_count)
            else bool(random.getrandbits(1)),
        }

    any_type_columns = [
        "date",
        "null_count_str",
        "null_percent_str",
        "null_count_float",
        "null_percent_float",
        "null_count_int",
        "null_percent_int",
        "null_count_bool",
        "null_percent_bool",
    ]
    dates = generate_date_range(
        base_date=datetime.today() - timedelta(days=2), numdays=30
    )
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "training", "any_type_column_anomalies_training.csv"
        ),
        training_rows,
        any_type_columns,
    )

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "any_type_column_anomalies_validation.csv"
        ),
        validation_rows,
        any_type_columns,
    )


def generate_dimension_anomalies_training_and_validation_files(rows_count_per_day=300):
    def get_training_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "platform": "windows"
            if row_index < (10 / 100 * rows_count)
            else ("android" if row_index < (55 / 100 * rows_count) else "ios"),
            "version": row_index % 3,
            "user_id": random.randint(1, rows_count),
        }

    def get_validation_row(date, row_index, rows_count):
        return {
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "platform": "windows"
            if row_index < (99 / 100 * rows_count)
            else random.choice(["android", "ios"]),
            "version": row_index % 3,
            "user_id": random.randint(1, rows_count),
        }

    dimension_columns = ["date", "platform", "version", "user_id"]
    dates = generate_date_range(
        base_date=datetime.today() - timedelta(days=2), numdays=30
    )
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(
        os.path.join(FILE_DIR, "data", "training", "dimension_anomalies_training.csv"),
        training_rows,
        dimension_columns,
    )

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(
        rows_count_per_day, [validation_date], get_validation_row
    )
    write_rows_to_csv(
        os.path.join(
            FILE_DIR, "data", "validation", "dimension_anomalies_validation.csv"
        ),
        validation_rows,
        dimension_columns,
    )


def generate_fake_data():
    print("Generating fake data!")
    generate_string_anomalies_training_and_validation_files()
    generate_numeric_anomalies_training_and_validation_files()
    generate_any_type_anomalies_training_and_validation_files()
    generate_dimension_anomalies_training_and_validation_files()


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


def e2e_tests(target, test_types, clear_tests) -> TestResults:
    test_results = TestResults()

    dbt_runner = DbtRunner(
        project_dir=FILE_DIR,
        profiles_dir=os.path.join(expanduser("~"), ".dbt"),
        target=target,
        raise_on_failure=False,
    )

    if clear_tests:
        clear_test_logs = dbt_runner.run_operation(macro_name="clear_tests")
        for clear_test_log in clear_test_logs:
            print(clear_test_log)

    dbt_runner.seed(select="training")

    dbt_runner.run(full_refresh=True)

    if "table" in test_types:
        dbt_runner.test(select="tag:table_anomalies")
        results = [
            TestResult(type="table_anomalies", message=msg)
            for msg in dbt_runner.run_operation(macro_name="validate_table_anomalies")
        ]
        test_results.extend(results)

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
        current_time = datetime.now()
        # Run operation returns the operation value as a list of strings.
        # So we convert the days_back value into int.
        days_back_project_var = int(
            dbt_runner.run_operation(
                macro_name="return_config_var", macro_args={"var_name": "days_back"}
            )[0]
        )
        # No need to create todays metric because the validation run does it.
        for run_index in range(1, days_back_project_var):
            custom_run_time = (
                current_time - timedelta(days_back_project_var - run_index)
            ).isoformat()
            dbt_runner.test(
                select="tag:no_timestamp",
                vars={"custom_run_started_at": custom_run_time},
            )

    dbt_runner.seed(select="validation")
    dbt_runner.run()

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
        dbt_runner.seed(select="schema_changes_data")
        dbt_runner.test(select="tag:schema_changes")
        dbt_runner.seed(select="schema_changes_validation")
        schema_changes_logs = dbt_runner.run_operation(
            macro_name="do_schema_changes", log_errors=True
        )
        for schema_changes_log in schema_changes_logs:
            print(schema_changes_log)
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
    help="table / column / schema / regular / artifacts / error_test / error_model / error_snapshot / dimension / no_timestamp / debug / all (default = all)",
)
@click.option(
    "--generate-data",
    "-g",
    type=bool,
    default=True,
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
        ]
    else:
        e2e_types = [e2e_type]

    all_results = {}
    found_failures = False
    for e2e_target in e2e_targets:
        print(f"Starting {e2e_target} tests\n")
        e2e_test_results = e2e_tests(e2e_target, e2e_types, clear_tests)
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
