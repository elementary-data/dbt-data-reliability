from datetime import date, datetime, time, timedelta

DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def generate_dates(base_date, step=None, days_back=31):
    step = step or timedelta(days=1)
    if type(base_date) is date and step.days == 0:
        # This is needed to avoid an infinite loop
        base_date = datetime.combine(base_date, time.min)

    min_date = base_date - timedelta(days=days_back)
    dates = []
    while base_date > min_date:
        dates.append(base_date)
        base_date = base_date - step
    return dates
