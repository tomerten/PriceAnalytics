# -*- coding: utf-8 -*-

"""
Package priceana
=======================================

Top-level package for priceana.
"""

__version__ = "0.0.0"

import asyncio
import time
from asyncio import Semaphore, futures
from collections import namedtuple
from datetime import timedelta
from re import I
from typing import List, Tuple

from aiohttp import ClientSession
from termcolor import colored

from .constants import (
    all_keys,
    daily_keys,
    monthly_keys,
    quarterly_keys,
    valid_intervals,
    valid_periods,
    weekly_keys,
    yearly_keys,
)
from .utils.AsyncUtils import aparse_yahoo_prices, store_yahoo_financial_data, store_yahoo_prices
from .utils.DataBroker import DataBrokerMongoDb
from .utils.DateTimeUtils import validate_date
from .utils.LoggingUtils import logger
from .utils.UrlUtils import generate_combinations, generate_price_params, generate_price_urls


class InvalidPeriodError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "InvalidPeriodError, {0} ".format(self.message)
        else:
            return "InvalidPeriodError: Invalid period for price time-series."


class InvalidIntervalError(Exception):
    def __init__(self, *args):
        if args:
            self.message = args[0]
        else:
            self.message = None

    def __str__(self):
        if self.message:
            return "InvalidIntervalError, {0} ".format(self.message)
        else:
            return "InvalidIntervalError: Invalid interval for price time-series."


class YahooPrices:
    """Class for downloading price, cleaning, storing price data."""

    def __init__(self, tickers: List[str], databroker: DataBrokerMongoDb, *args, **kwargs):
        self._tickers = tickers
        self._databroker = databroker
        self._data = None

        # PERIOD TO USE FOR THE PRICE DATA
        self._period = kwargs.get("period", "max")

        # TIME INTERVAL TO USE FOR THE PRICE DATA (1m, 1d, ...)
        self._interval = kwargs.get("interval", "all")

        # START AND END DATES
        self._start = kwargs.get("start", None)
        self._end = kwargs.get("end", None)

        # VERIFY INPUT DATA
        self._input_validation()

    def _input_validation(self) -> None:
        """
        Private method to assert of
        input is valid.
        """
        if not isinstance(self._tickers, list):
            self._tickers = [self._tickers]

        if self._period not in valid_periods:
            raise InvalidPeriodError

        if self._interval not in valid_intervals:
            raise InvalidIntervalError

        # if self._financialperiod not in self.FINPERIOD.keys():
        #   raise InvalidPeriodError("Invalid period for financial data!")

        if not isinstance(self._databroker, DataBrokerMongoDb):
            raise TypeError

        if self._start:
            validate_date(self._start)

        if self._end:
            validate_date(self._end)

    @property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

    async def _download(self):
        # GENERATE URLS AND PARAMETER DICTS FOR PRICE DATA
        price_urls = generate_price_urls(self._tickers)
        price_params = generate_price_params(self._period, self._interval, self._start, self._end)

        # GENERATE THE URL-PARAMETER TUPLE COMBINATIONS
        # FOR BOTH PRICE AND FINANCIAL DATA
        pricecombinations = generate_combinations(price_urls, price_params)

        sem = Semaphore(1000)

        pricetasks = []

        async with ClientSession() as session:
            for tup in pricecombinations:
                task = asyncio.ensure_future(aparse_yahoo_prices(sem, tup, session))
                pricetasks.append(task)

            res = await asyncio.gather(*pricetasks)
        return res

    def download(self):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(self._download())
        res = loop.run_until_complete(future)
        self.data = res

    def __repr__(self):
        return "<tickers> : {}, <period>: {}, <interval>: {}, <start>: {}, <end>: {}".format(
            self._tickers,
            self._period,
            self._interval,
            self._start,
            self._end,
        )
