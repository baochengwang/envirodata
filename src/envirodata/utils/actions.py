import datetime
import numpy as np


class Hourly:
    def __init__(self, date):
        self.start_date = date - datetime.timedelta(hours=1)
        self.end_date = date


class Daily:
    def __init__(self, date):
        self.start_date = date - datetime.timedelta(days=1)
        self.end_date = date


class DayRange:
    def __init__(self, date, days):
        self.start_date = date - datetime.timedelta(days=days)
        self.end_date = date


class Average:
    def stat(self, values):
        return np.average(values)


class DailyAverage(Daily, Average):
    def __init__(self, date):
        Daily.__init__(self, date)
        Average.__init__(self)

        self.name = "daily_average"


class DayRangeAverage(DayRange, Average):
    def __init__(self, date, days):
        DayRange.__init__(self, date, days)
        Average.__init__(self)

        self.name = "daily_average"
