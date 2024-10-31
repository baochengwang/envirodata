import datetime
from typing import Callable
from dataclasses import dataclass
import copy

import numpy as np


@dataclass
class Statistic:
    """Parameters of a statistic to be calculated for a given variable.

    :param name: (Arbitrary) name of this statistic, used in config.
    :type name: str
    :param begin: Start of the statistics period (e.g., yesterday)
    :type begin: datetime.timedelta
    :param end: End time of the statistics period (e.g., today)
    :type end: datetime.timedelta
    :param function: Function to apply over the retrieved data (e.g., np.nanmean)
    :type function: Callable
    :param daily: Daily aggregates?
    :type daily: bool
    """

    name: str
    begin: datetime.timedelta
    end: datetime.timedelta
    function: Callable
    daily: bool = False


def shifted_difference(values: list[float], shift: int) -> list[float]:
    """Calculate difference values between "values" and "values" shifted by "shift"
    items. Think "3 hour pressure differences".

    :param values: Data value series
    :type values: list[float]
    :param shift: Number of indices to shift the value series
    :type shift: int
    :return: Difference value series
    :rtype: _list[float]
    """

    old = np.concatenate((values, np.array([np.nan] * shift)))
    new = np.concatenate((np.array([np.nan] * shift), values))

    delta = old - new

    return delta


def mda8(times, values):
    current = copy.copy(min(times)).replace(hour=0, minute=0, second=0)
    end = copy.copy(current).replace(hour=23, minute=59, second=59)

    result = []
    while (current + datetime.timedelta(hours=8)) < end:
        eight_mask = np.logical_and(
            (times >= current), (times < (current + datetime.timedelta(hours=8)))
        )
        eight_values = np.nanmean(values[eight_mask])
        result.append(eight_values)
        current += datetime.timedelta(hours=1)

    return np.nanmax(result)


def daybased(
    times: list[datetime.datetime],
    values: list[float],
    daily_function: Callable,
    total_function: Callable,
) -> float:
    # we start at 0 hours
    day_start_date = copy.copy(min(times))
    day_start_date = day_start_date.replace(hour=0, minute=0, second=0, microsecond=0)

    end_date = copy.copy(max(times))
    end_date = end_date.replace(
        hour=0, minute=0, second=0, microsecond=0
    ) + datetime.timedelta(days=1)

    result = []

    times_arr = np.array(times)
    values_arr = np.array(values)

    while day_start_date < end_date:
        day_end_date = copy.copy(day_start_date).replace(hour=23, minute=59, second=59)
        day_mask = np.logical_and(
            (times_arr >= day_start_date), (times_arr < day_end_date)
        )
        day_times = times_arr[day_mask]
        day_values = values_arr[day_mask]

        result.append(daily_function(day_times, day_values))
        day_start_date += datetime.timedelta(days=1)

    return total_function(result)


def amplitude(values):
    return np.nanmax(values) - np.nanmin(values)


AvailableStatistics = [
    Statistic(
        "current",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: values[0],
    ),
    Statistic(
        "day_min",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: daybased(
            times, values, lambda t, v: np.nanmin(v), np.nanmin
        ),
        True,
    ),
    Statistic(
        "day_mean",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: daybased(
            times, values, lambda t, v: np.nanmean(v), np.nanmean
        ),
        True,
    ),
    Statistic(
        "day_max",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: daybased(
            times, values, lambda t, v: np.nanmax(v), np.nanmax
        ),
        True,
    ),
    Statistic(
        "day_sum",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: daybased(
            times, values, lambda t, v: np.nansum(v), np.nansum
        ),
        True,
    ),
    Statistic(
        "24h_amplitude",
        datetime.timedelta(days=-1),
        datetime.timedelta(days=0),
        lambda times, values: amplitude(values),
    ),
    Statistic(
        "24h_max_3h_delta",
        datetime.timedelta(days=-1),
        datetime.timedelta(days=0),
        lambda times, values: np.nanmax(np.abs(shifted_difference(values, 3))),
    ),
    Statistic(
        "5day_max_3h_delta",
        datetime.timedelta(days=-5),
        datetime.timedelta(days=0),
        lambda times, values: np.nanmax(np.abs(shifted_difference(values, 3))),
    ),
    Statistic(
        "mda8",
        datetime.timedelta(days=0),
        datetime.timedelta(days=0),
        lambda times, values: daybased(times, values, mda8, np.nanmean),
        True,
    ),
    Statistic(
        "3day_mean_mda8",
        datetime.timedelta(days=-3),
        datetime.timedelta(days=0),
        lambda times, values: daybased(times, values, mda8, np.nanmean),
        True,
    ),
    Statistic(
        "7day_mean_mda8",
        datetime.timedelta(days=-7),
        datetime.timedelta(days=0),
        lambda times, values: daybased(times, values, mda8, np.nanmean),
        True,
    ),
]

for day in [3, 5, 7]:
    AvailableStatistics += [
        Statistic(
            f"{day}day_min",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: np.nanmin(values),
            True,
        ),
        Statistic(
            f"{day}day_mean",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: np.nanmean(values),
            True,
        ),
        Statistic(
            f"{day}day_max",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: np.nanmax(values),
            True,
        ),
        Statistic(
            f"{day}day_mean_day_max",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: daybased(
                times, values, lambda t, v: np.nanmax(v), np.nanmean
            ),
            True,
        ),
        Statistic(
            f"{day}day_max_day_max",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: daybased(
                times, values, lambda t, v: np.nanmax(v), np.nanmax
            ),
            True,
        ),
        Statistic(
            f"{day}day_mean_day_min",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: daybased(
                times, values, lambda t, v: np.nanmin(v), np.nanmean
            ),
            True,
        ),
        Statistic(
            f"{day}day_min_day_min",
            datetime.timedelta(days=-day),
            datetime.timedelta(days=0),
            lambda times, values: daybased(
                times, values, lambda t, v: np.nanmin(v), np.nanmin
            ),
            True,
        ),
    ]
