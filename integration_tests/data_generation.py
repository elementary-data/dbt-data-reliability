import csv
from datetime import datetime, timedelta
import random
import string

any_type_columns = ['date', 'null_count', 'null_percent']
numeric_columns = ['date', 'min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance']


def generate_date_range(base_date, numdays=30):
    return [base_date - timedelta(days=x) for x in range(0, numdays)]


"""
 column_any_type:
      - null_count
      - null_percent
"""
def generate_string_anomalies_training_and_validation_files(rows_count_per_day=100,
                                                            base_date=datetime.today() - timedelta(days=2),
                                                            numdays=30):
    """
      - min_length
      - max_length
      - average_length
      - missing_count
      - missing_percent
    """
    string_columns = ['date', 'min_length', 'max_length', 'average_length', 'missing_count', 'missing_percent']
    dates = generate_date_range(base_date=base_date, numdays=numdays)
    rows = []
    for date in dates:
        for i in range(0, rows_count_per_day):
            row = {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                   'min_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10))),
                   'max_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 10))),
                   'average_length': ''.join(random.choices(string.ascii_lowercase, k=5)),
                   'missing_count': '' if i < 3 else ''.join(random.choices(string.ascii_lowercase, k=5)),
                   'missing_percent': '' if random.randint(1, 100) <= 20 else ''.join(random.choices(string.ascii_lowercase, k=5))}
            rows.append(row)

    with open('./integration_tests/data/training/string_column_anomalies_training.csv', 'w') as training_file:
        writer = csv.DictWriter(training_file, fieldnames=string_columns)
        writer.writeheader()
        writer.writerows(rows)

    validation_date = datetime.today() - timedelta(days=1)
    rows = []
    for i in range(0, rows_count_per_day):
        row = {'date': validation_date.strftime('%Y-%m-%d %H:%M:%S'),
               'min_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(1, 10))),
               'max_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 15))),
               'average_length': ''.join(random.choices(string.ascii_lowercase, k=random.randint(5, 8))),
               'missing_count': '' if i < 5 else ''.join(random.choices(string.ascii_lowercase, k=5)),
               'missing_percent': '' if random.randint(1, 100) <= 40 else ''.join(random.choices(string.ascii_lowercase, k=5))}
        rows.append(row)

    with open('./integration_tests/data/validation/string_column_anomalies_validation.csv', 'w') as validation_file:
        writer = csv.DictWriter(validation_file, fieldnames=string_columns)
        writer.writeheader()
        writer.writerows(rows)

def generate_numeric_anomalies_training_and_validation_files(rows_count_per_day=200,
                                                             base_date=datetime(2022, 1, 22, 8, 10, 2),
                                                             numdays=20):
    """
        - min
        - max
        - zero_count
        - zero_percent
        - average
        - standard_deviation
        - variance
    """
    numeric_columns = ['date', 'min', 'max', 'zero_count', 'zero_percent', 'average', 'standard_deviation', 'variance']
    dates = generate_date_range(base_date=base_date, numdays=numdays)
    rows = []
    for date in dates:
        for i in range(0, rows_count_per_day):
            row = {'date': date.strftime('%Y-%m-%d %H:%M:%S'),
                   'min': random.randint(100, 200),
                   'max': random.randint(100, 200),
                   'zero_count': 0 if i < 5 else random.randint(100, 200),
                   'zero_percent': 0 if random.randint(1, 10) <= 2 else random.randint(100, 200),
                   'average': random.randint(100, 102),
                   'standard_deviation': None,
                   'variance': None}
            rows.append(row)

generate_string_anomalies_training_and_validation_files()
generate_numeric_anomalies_training_and_validation_files()