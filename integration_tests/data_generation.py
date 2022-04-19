import csv
from datetime import datetime, timedelta
import random
import string
import os

any_type_columns = ['date', 'null_count', 'null_percent']

FILE_DIR = os.path.dirname(__file__)


def generate_date_range(base_date, numdays=30):
    return [base_date - timedelta(days=x) for x in range(0, numdays)]


def write_rows_to_csv(csv_path, rows, header):
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
                'missing_count': '' if row_index < (3 / 100 * rows_count) else ''.join(random.choices(string.ascii_lowercase, k=5)),
                'missing_percent': '' if random.randint(1, rows_count) <= (20 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5))}

    def get_validation_row(date, row_index, rows_count):
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 10))),
                'max_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15))),
                'average_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 8))),
                'missing_count': '' if row_index < (5 / 100 * rows_count) else ''.join(random.choices(string.ascii_lowercase, k=5)),
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
                'zero_percent': 0 if random.randint(1, rows_count) <= (20 / 100 * rows_count) else random.randint(100, 200),
                'average': random.randint(99, 101),
                'standard_deviation': random.randint(99, 101),
                'variance': random.randint(99, 101)}

    def get_validation_row(date, row_index, rows_count):
        row_index += -(rows_count / 2)
        return {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                'min': random.randint(10, 200),
                'max': random.randint(100, 300),
                'zero_count': 0 if row_index < (3 / 100 * rows_count) else random.randint(100, 200),
                'zero_percent': 0 if random.randint(1, rows_count) <= (40 / 100 * rows_count) else random.randint(100, 200),
                'average': random.randint(99, 104),
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
                'null_count_str': None if row_index < (5 / 100 * rows_count) else
                ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_percent_str': None if random.randint(1, rows_count) <= (40 / 100 * rows_count)
                else ''.join(random.choices(string.ascii_lowercase, k=5)),
                'null_count_float': None if row_index < (5 / 100 * rows_count) else random.uniform(1.2, 8.9),
                'null_percent_float': None if random.randint(1, rows_count) <= (40 / 100 * rows_count)
                else random.uniform(1.2, 8.9),
                'null_count_int': None if row_index < (5 / 100 * rows_count) else random.randint(100, 200),
                'null_percent_int': None if random.randint(1, rows_count) <= (40 / 100 * rows_count)
                else random.randint(100, 200),
                'null_count_bool': None if row_index < (5 / 100 * rows_count) else bool(random.getrandbits(1)),
                'null_percent_bool': None if random.randint(1, rows_count) <= (40 / 100 * rows_count)
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


def main():
    generate_string_anomalies_training_and_validation_files()
    generate_numeric_anomalies_training_and_validation_files()
    generate_any_type_anomalies_training_and_validation_files()


if __name__ == '__main__':
    main()
