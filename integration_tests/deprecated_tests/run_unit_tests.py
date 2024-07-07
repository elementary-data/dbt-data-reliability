import glob
import os
from os.path import expanduser

import click
from elementary.clients.dbt.factory import create_dbt_runner

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def get_unit_tests(test_file=None):
    unit_test_file_paths = glob.glob(
        os.path.join(FILE_DIR, "macros", "unit_tests", "test_*.sql"), recursive=True
    )
    unit_tests = []
    for unit_test_file_path in unit_test_file_paths:
        if test_file is not None:
            if test_file in unit_test_file_path:
                unit_tests.append(
                    os.path.basename(unit_test_file_path).replace(".sql", "")
                )
        else:
            unit_tests.append(os.path.basename(unit_test_file_path).replace(".sql", ""))

    return unit_tests


def print_unit_test_results(unit_test, unit_test_results):
    print(f"\n{unit_test}:")
    for i in range(0, len(unit_test_results)):
        print(f"{i + 1}.{unit_test_results[i]}")


def run_unit_tests(test_file, target="snowflake"):
    dbt_runner = create_dbt_runner(
        project_dir=FILE_DIR,
        profiles_dir=os.path.join(expanduser("~"), ".dbt"),
        target=target,
    )
    unit_tests = get_unit_tests(test_file)
    print(f"Running unit tests against target - {target}")
    for unit_test in unit_tests:
        unit_test_results = dbt_runner.run_operation(
            macro_name=unit_test, log_errors=True, return_raw_edr_logs=True
        )
        print_unit_test_results(unit_test, unit_test_results)


@click.command()
@click.option(
    "--target",
    "-t",
    type=str,
    default="postgres",
)
@click.option(
    "--test-file", "-f", type=str, default=None, help="The name of tests file to run"
)
def main(target, test_file):
    run_unit_tests(test_file, target)


if __name__ == "__main__":
    main()
