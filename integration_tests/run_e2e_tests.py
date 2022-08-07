import csv
import os
import random
import string
from datetime import datetime, timedelta
from os.path import expanduser
from pathlib import Path

import click
from clients.dbt.dbt_runner import DbtRunner

any_type_columns = ['date', 'null_count', 'null_percent']

FILE_DIR = os.path.dirname(os.path.realpath(__file__))


def generate_date_range(base_date, numdays=30):
    return [base_date - timedelta(days=x) for x in range(0, numdays)]


def write_rows_to_csv(csv_path, rows, header):
    # Creates the csv file directories if needed.
    directory_path = Path(csv_path).parent.resolve()
    Path(directory_path).mkdir(parents=True, exist_ok=True)

    with open(csv_path, 'w') as csv_file:
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
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10))),
                'max_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10))),
                'average_length': ''.join(random.choices(string.ascii_lowercase, k=5)),
                'missing_count': '' if row_index < (3 / 100 * rows_count) else ''.join(
                    random.choices(string.ascii_lowercase, k=5)),
                'missing_percent': '' if random.randint(1, rows_count) <= (20 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5))}

    def get_validation_row(date, row_index, rows_count):
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 10))),
                'max_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15))),
                'average_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 8))),
                'missing_count': '' if row_index < (20 / 100 * rows_count) else ''.join(
                    random.choices(string.ascii_lowercase, k=5)),
                'missing_percent': '' if random.randint(1, rows_count) <= (60 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5))}

    string_columns = ['date', 'min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent']
    dates = generate_date_range(base_date=datetime.today() - timedelta(days=2), numdays=30)
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'training', 'string_column_anomalies_training.csv'),
                      training_rows,
                      string_columns)

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(rows_count_per_day, [validation_date], get_validation_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'validation', 'string_column_anomalies_validation.csv'),
                      validation_rows,
                      string_columns)


def generate_numeric_anomalies_training_and_validation_files(rows_count_per_day=200):
    def get_training_row(date, row_index, rows_count):
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min': random.randint(100, 200),
                'max': random.randint(100, 200),
                'zero_count': 0 if row_index < (3 / 100 * rows_count) else random.randint(100, 200),
                'zero_percent': 0 if random.randint(1, rows_count) <= (20 / 100 * rows_count) else random.randint(100,
                                                                                                                  200),
                'average': random.randint(99, 101),
                'standard_deviation': random.randint(99, 101),
                'variance': random.randint(99, 101)}

    def get_validation_row(date, row_index, rows_count):
        row_index += -(rows_count / 2)
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min': random.randint(10, 200),
                'max': random.randint(100, 300),
                'zero_count': 0 if row_index < (80 / 100 * rows_count) else random.randint(100, 200),
                'zero_percent': 0 if random.randint(1, rows_count) <= (60 / 100 * rows_count) else random.randint(100,
                                                                                                                  200),
                'average': random.randint(101, 110),
                'standard_deviation': random.randint(80, 120),
                'variance': random.randint(80, 120)}

    numeric_columns = ['date', 'min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance']
    dates = generate_date_range(base_date=datetime.today() - timedelta(days=2), numdays=30)
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'training', 'numeric_column_anomalies_training.csv'),
                      training_rows,
                      numeric_columns)

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(rows_count_per_day, [validation_date], get_validation_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'validation', 'numeric_column_anomalies_validation.csv'),
                      validation_rows,
                      numeric_columns)


def generate_any_type_anomalies_training_and_validation_files(rows_count_per_day=300):
    def get_training_row(date, row_index, rows_count):
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'null_count_str': None if row_index < (3 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_percent_str': None if random.randint(1, rows_count) <= (20 / 100 * rows_count)
                else ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_count_float': None if row_index < (3 / 100 * rows_count) else random.uniform(1.2, 8.9),
                'null_percent_float': None if random.randint(1, rows_count) <= (20 / 100 * rows_count)
                else random.uniform(1.2, 8.9),
                'null_count_int': None if row_index < (3 / 100 * rows_count) else random.randint(100, 200),
                'null_percent_int': None if random.randint(1, rows_count) <= (20 / 100 * rows_count)
                else random.randint(100, 200),
                'null_count_bool': None if row_index < (3 / 100 * rows_count) else bool(random.getrandbits(1)),
                'null_percent_bool': None if random.randint(1, rows_count) <= (20 / 100 * rows_count)
                else bool(random.getrandbits(1))}

    def get_validation_row(date, row_index, rows_count):
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'null_count_str': None if row_index < (80 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_percent_str': None if random.randint(1, rows_count) <= (60 / 100 * rows_count)
                else ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_count_float': None if row_index < (80 / 100 * rows_count) else random.uniform(1.2, 8.9),
                'null_percent_float': None if random.randint(1, rows_count) <= (60 / 100 * rows_count)
                else random.uniform(1.2, 8.9),
                'null_count_int': None if row_index < (80 / 100 * rows_count) else random.randint(100, 200),
                'null_percent_int': None if random.randint(1, rows_count) <= (60 / 100 * rows_count)
                else random.randint(100, 200),
                'null_count_bool': None if row_index < (80 / 100 * rows_count) else bool(random.getrandbits(1)),
                'null_percent_bool': None if random.randint(1, rows_count) <= (60 / 100 * rows_count)
                else bool(random.getrandbits(1))}

    any_type_columns = ['date', 'null_count_str', 'null_percent_str', 'null_count_float', 'null_percent_float',
                        'null_count_int', 'null_percent_int', 'null_count_bool', 'null_percent_bool']
    dates = generate_date_range(base_date=datetime.today() - timedelta(days=2), numdays=30)
    training_rows = generate_rows(rows_count_per_day, dates, get_training_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'training', 'any_type_column_anomalies_training.csv'),
                      training_rows,
                      any_type_columns)

    validation_date = datetime.today() - timedelta(days=1)
    validation_rows = generate_rows(rows_count_per_day, [validation_date], get_validation_row)
    write_rows_to_csv(os.path.join(FILE_DIR, 'data', 'validation', 'any_type_column_anomalies_validation.csv'),
                      validation_rows,
                      any_type_columns)


def generate_fake_data():
    print('Generating fake data!')
    generate_string_anomalies_training_and_validation_files()
    generate_numeric_anomalies_training_and_validation_files()
    generate_any_type_anomalies_training_and_validation_files()


def e2e_tests(target, test_types, clear_tests):
    table_test_results = []
    string_column_anomalies_test_results = []
    numeric_column_anomalies_test_results = []
    any_type_column_anomalies_test_results = []
    schema_changes_test_results = []
    regular_test_results = []
    artifacts_results = []

    dbt_runner = DbtRunner(project_dir=FILE_DIR, profiles_dir=os.path.join(expanduser('~'), '.dbt'), target=target)

    if clear_tests:
        clear_test_logs = dbt_runner.run_operation(macro_name='clear_tests')
        for clear_test_log in clear_test_logs:
            print(clear_test_log)

    dbt_runner.seed(select='training')

    dbt_runner.run(full_refresh=True)

    if 'table' in test_types and target != 'databricks':
        dbt_runner.test(select='tag:table_anomalies')
        table_test_results = dbt_runner.run_operation(macro_name='validate_table_anomalies')
        print_test_result_list(table_test_results)
        # If only table tests were selected no need to continue to the rest of the flow
        if len(test_types) == 1:
            return [table_test_results, string_column_anomalies_test_results, numeric_column_anomalies_test_results,
                    any_type_column_anomalies_test_results, schema_changes_test_results, regular_test_results,
                    artifacts_results]

    if 'error_test' in test_types:
        dbt_runner.test(select='tag:error_test')
        error_test_results = dbt_runner.run_operation(macro_name='validate_error_test')
        print_test_result_list(error_test_results)

    if 'error_model' in test_types:
        dbt_runner.run(select='tag:error_model')
        error_test_results = dbt_runner.run_operation(macro_name='validate_error_model')
        print_test_result_list(error_test_results)

    if 'error_snapshot' in test_types:
        dbt_runner.snapshot()
        error_test_results = dbt_runner.run_operation(macro_name='validate_error_snapshot')
        print_test_result_list(error_test_results)

    # Creates row_count metrics for anomalies detection.
    if 'no_timestamp' in test_types and target != 'databricks':
        current_time = datetime.now()
        # Run operation returns the operation value as a list of strings.
        # So we convert the days_back value into int.
        days_back_project_var = int(
            dbt_runner.run_operation(macro_name="return_config_var", macro_args={"var_name": "days_back"})[0])
        # No need to create todays metric because the validation run does it.
        for run_index in range(1, days_back_project_var):
            custom_run_time = (current_time - timedelta(days_back_project_var - run_index)).isoformat()
            dbt_runner.test(select='tag:no_timestamp', vars={"custom_run_started_at": custom_run_time})

    dbt_runner.seed(select='validation')
    dbt_runner.run()

    if 'debug' in test_types:
        dbt_runner.test(select='tag:debug')
        return [table_test_results, string_column_anomalies_test_results, numeric_column_anomalies_test_results,
                any_type_column_anomalies_test_results, schema_changes_test_results, regular_test_results,
                artifacts_results]

    if 'no_timestamp' in test_types and target != 'databricks':
        dbt_runner.test(select='tag:no_timestamp')
        no_timestamp_test_results = dbt_runner.run_operation(macro_name='validate_no_timestamp_anomalies')
        print_test_result_list(no_timestamp_test_results)

    if 'column' in test_types and target != 'databricks':
        dbt_runner.test(select='tag:string_column_anomalies')
        string_column_anomalies_test_results = dbt_runner.run_operation(macro_name='validate_string_column_anomalies')
        print_test_result_list(string_column_anomalies_test_results)
        dbt_runner.test(select='tag:numeric_column_anomalies')
        numeric_column_anomalies_test_results = dbt_runner.run_operation(macro_name='validate_numeric_column_anomalies')
        print_test_result_list(numeric_column_anomalies_test_results)
        dbt_runner.test(select='tag:all_any_type_columns_anomalies')
        any_type_column_anomalies_test_results = dbt_runner.run_operation(macro_name=
                                                                          'validate_any_type_column_anomalies')
        print_test_result_list(any_type_column_anomalies_test_results)

    if 'schema' in test_types and target != 'databricks':
        dbt_runner.seed(select='schema_changes_data')
        dbt_runner.test(select='tag:schema_changes')
        dbt_runner.seed(select='schema_changes_validation')
        schema_changes_logs = dbt_runner.run_operation(macro_name='do_schema_changes', log_errors=True)
        for schema_changes_log in schema_changes_logs:
            print(schema_changes_log)
        dbt_runner.test(select='tag:schema_changes')
        schema_changes_test_results = dbt_runner.run_operation(macro_name='validate_schema_changes')
        print_test_result_list(schema_changes_test_results)

    if 'regular' in test_types:
        dbt_runner.test(select='test_type:singular tag:regular_tests')
        regular_test_results = dbt_runner.run_operation(macro_name='validate_regular_tests')
        print_test_result_list(regular_test_results)

    if 'artifacts' in test_types:
        artifacts_results = dbt_runner.run_operation(macro_name='validate_dbt_artifacts')
        print_test_result_list(artifacts_results)

    return [table_test_results, string_column_anomalies_test_results, numeric_column_anomalies_test_results,
            any_type_column_anomalies_test_results, schema_changes_test_results, regular_test_results,
            artifacts_results]


def print_test_result_list(test_results):
    for test_result in test_results:
        print(test_result)


def print_tests_results(table_test_results,
                        string_column_anomalies_test_results,
                        numeric_column_anomalies_test_results,
                        any_type_column_anomalies_test_results,
                        schema_changes_test_results,
                        regular_test_results,
                        artifacts_results):
    print('\nTable test results')
    print_test_result_list(table_test_results)
    print('\nString columns test results')
    print_test_result_list(string_column_anomalies_test_results)
    print('\nNumeric columns test results')
    print_test_result_list(numeric_column_anomalies_test_results)
    print('\nAny type columns test results')
    print_test_result_list(any_type_column_anomalies_test_results)
    print('\nSchema changes test results')
    print_test_result_list(schema_changes_test_results)
    print('\nRegular test results')
    print_test_result_list(regular_test_results)
    print('\ndbt artifacts results')
    print_test_result_list(artifacts_results)


@click.command()
@click.option(
    '--target', '-t',
    type=str,
    default='all',
    help="snowflake / bigquery / redshift / all (default = all)"
)
@click.option(
    '--e2e-type', '-e',
    type=str,
    default='all',
    help="table / column / schema / regular / artifacts / error_test / error_model / error_snapshot / no_timestamp / debug / all (default = all)"
)
@click.option(
    '--generate-data', '-g',
    type=bool,
    default=True,
    help="Set to true if you want to re-generate fake data (default = True)"
)
@click.option(
    '--clear-tests',
    type=bool,
    default=True,
    help="Set to true if you want to clear the tests (default = True)"
)
def main(target, e2e_type, generate_data, clear_tests):
    if generate_data:
        generate_fake_data()

    if target == 'all':
        e2e_targets = ['snowflake', 'bigquery', 'redshift']
    else:
        e2e_targets = [target]

    if e2e_type == 'all':
        e2e_types = ['table', 'column', 'schema', 'regular', 'artifacts', 'error_test', 'error_model', 'error_snapshot']
    else:
        e2e_types = [e2e_type]

    all_results = {}
    for e2e_target in e2e_targets:
        print(f'Starting {e2e_target} tests\n')
        e2e_test_results = e2e_tests(e2e_target, e2e_types, clear_tests)
        print(f'\n{e2e_target} results')
        all_results[e2e_target] = e2e_test_results

    for e2e_target, e2e_test_results in all_results.items():
        print(f'\n{e2e_target} results')
        print_tests_results(*e2e_test_results)


if __name__ == '__main__':
    main()