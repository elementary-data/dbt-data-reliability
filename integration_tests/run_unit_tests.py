import os
from os.path import expanduser
from monitor.dbt_runner import DbtRunner
import glob

FILE_DIR = os.path.dirname(__file__)


def get_unit_tests():
    unit_test_file_paths = glob.glob(os.path.join(FILE_DIR, 'macros', 'unit_tests', 'test_*.sql'), recursive=True)
    unit_tests = []
    for unit_test_file_path in unit_test_file_paths:
        unit_tests.append(os.path.basename(unit_test_file_path).replace('.sql', ''))
    return unit_tests


def print_unit_test_results(unit_test, unit_test_results):
    print(f'\n{unit_test}:')
    for i in range(0, len(unit_test_results)):
        print(f'{i+1}.{unit_test_results[i]}')


def run_unit_tests(target='snowflake'):
    dbt_runner = DbtRunner(project_dir=FILE_DIR, profiles_dir=os.path.join(expanduser('~'), '.dbt'), target=target)
    unit_tests = get_unit_tests()
    for unit_test in unit_tests:
        unit_test_results = dbt_runner.run_operation(macro_name=unit_test)
        print_unit_test_results(unit_test, unit_test_results)


def main():
    run_unit_tests()


if __name__ == '__main__':
    main()
