# -*- coding: utf-8 -*-

"""
Module priceana.utils.DateTimeUtils
=================================================================

A module containing methods for date validation and cleaning.

"""

import time
from datetime import datetime as dt
from typing import Dict, Optional, Union


def validate_date(_date: Union[dt, str]) -> None:
    """Method to validate dates.

    Args:
        _date (Union[dt, str]): date to validate

    Raises:
        ValueError: raised if incorrect str format is given
        TypeError: raised if wrong input type is provided
    """
    if isinstance(_date, str):
        try:
            dt.strptime(_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Incorrect date str format")
    elif isinstance(_date, dt):
        pass
    else:
        raise TypeError("Invalid date")


def clean_start_end_period(
    start: Optional[Union[dt, str, int]],
    end: Optional[Union[dt, str, int]],
    period: Optional[str],
) -> Dict[str, Union[str, int]]:
    """Method to clean up start and end positions
    for getting yahoo historical price data.

    Args:
            start (Optional[Union[dt, str, int]]): start date
            end (Optional[Union[dt, str, int]]): end date
            period (Optional[str]): interval period of the time series

    Returns:
            Dict[str, Union[str, int]]: dictionary containing the cleaned parameters that will be used in the web request
    """

    if start or period is None or period.lower() == "max":
        if start is None:
            start = 0
        elif isinstance(start, dt):
            start = int(time.mktime(start.timetuple()))
        else:
            start = int(time.mktime(time.strptime(str(start), "%Y-%m-%d")))
        if end is None:
            end = int(time.time())
        elif isinstance(end, dt):
            end = int(time.mktime(end.timetuple()))
        else:
            end = int(time.mktime(time.strptime(str(end), "%Y-%m-%d")))

        # ANNOTATION NECESSARY TO PASS MYPY TESTS
        params: Dict[str, Union[str, int]] = {"period1": start, "period2": end}
    else:
        period = period.lower()
        params = {"range": period}

    return params
