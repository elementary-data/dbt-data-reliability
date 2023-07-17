from datetime import timedelta

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def generate_dates(base_date, period="days", count=1, days_back=30):
    min_date = base_date - timedelta(days=days_back)
    dates = []
    while base_date > min_date:
        dates.append(base_date)
        base_date = base_date - timedelta(**{period: count})
    return dates
