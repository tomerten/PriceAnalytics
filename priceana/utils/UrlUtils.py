# -*- coding: utf-8 -*-

"""
Module priceana.utils.UrlUtils
=================================================================

A module containing methods for download url generation.

"""

import itertools
from datetime import datetime as dt
from typing import List, Optional, Union

from ..constants import base_url, query_url, valid_intervals, valid_periods
from .DateTimeUtils import clean_start_end_period


def generate_yahoo_financial_data_urls(symbollist: list) -> list:
    """
    Method to generate yahoo financial
    data download urls.

    Ags:
        - symbollist (list): list of valid yahoo symbols

    Returns:
        - list: list of urls
    """
    if isinstance(symbollist, list) and symbollist != []:
        return [f"{query_url}{symbol}" for symbol in symbollist]
    else:
        raise TypeError


def generate_yahoo_financial_data_params(period_keys: list) -> List[dict]:
    """
    Private method to generate parameter dictionaries to pass to
    requests when downloading the financial data.

    Args:
        - period_keys (list): list of period keys to use
    Returns:
        List[Dict]: list of parameter dictionaries
    """
    if isinstance(period_keys, list) and period_keys != []:
        return [{"modules": ",".join(period_keys)}]
    else:
        raise TypeError


def generate_price_urls(symbollist: list) -> list:
    """
    Method to generate yahoo price urls.

    Args:
        - symbollist (list): list of valid yahoo symbols

    Returns:
        list : list of urls
    """
    return [f"{base_url}chart/{symbol}" for symbol in symbollist]


def generate_price_params(
    _period: str,
    _interval: str,
    _start: Optional[Union[dt, str, int]],
    _end: Optional[Union[dt, str, int]],
) -> List[dict]:
    """
    Private method to generate parameter dictionary to pass to requests for
    downloading the historical price data.

    Args:
        - _period (str): historical time period
        - _interval (str): time series interval value
        - _start (Optional[Union[dt, str, int]]): optional start date
        - _end (Optional[Union[dt, str, int]]): optional end date

    Returns:
        List[dict]: list of dictionaries with parameter settings
    """
    paramslist = []

    # CASE SINGLE INTERVAL IS REQUESTED
    if _interval != "all":
        # RESTRICT PERIODS IF NECESSARY
        if _interval == "1m":
            if valid_periods.index(_period) < valid_periods.index("5d"):
                _period = _period
            else:
                _period = "5d"

        elif _interval[-1] == "m" or _interval[-1] == "h":
            if valid_periods.index(_period) < valid_periods.index("1mo"):
                _period = _period
            else:
                _period = "1mo"
        elif _period is None:
            _period = "max"
        else:
            _period = _period

        # SET UP PARAMETERS
        params = clean_start_end_period(_start, _end, _period)
        params["includePrePost"] = 1
        params["events"] = "div,splits"
        params["interval"] = _interval.lower()

        paramslist.append(params.copy())

    # CASE ALL INTERVALS ARE REQUESTED
    else:
        for interval in valid_intervals[:-1]:
            # RESTRICT PERIODS IF NECESSARY
            if interval == "1m":
                if valid_periods.index(_period) < valid_periods.index("5d"):
                    _period = _period
                else:
                    _period = "5d"
            elif interval[-1] == "m" or interval[-1] == "h":
                if valid_periods.index(_period) < valid_periods.index("1mo"):
                    _period = _period
                else:
                    _period = "1mo"
            elif _period is None:
                _period = "max"
            else:
                _period = _period
            params = clean_start_end_period(_start, _end, _period)
            params["includePrePost"] = 1
            params["events"] = "div,splits"
            params["interval"] = interval.lower()

            paramslist.append(params.copy())

    return paramslist


def generate_combinations(urls: List[str], params: List[dict]) -> List[tuple]:
    """
    Private method to combine urls and parameter
    dictionaries in tuples to pass to the download
    method.

    Args:
        - urls (List[str]): list of urls
        - params (List[dict]): list of parameter dictionaries

    Returns:
        List[tuple]: list of tuples in the format (url, parameters)
    """
    # assert len(urls) == len(params)
    return list(itertools.product(urls, params))
