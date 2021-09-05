# -*- coding: utf-8 -*-

"""
Package priceana
=======================================

Top-level package for priceana.
"""

__version__ = "0.0.0"

import asyncio
import time
from asyncio import Semaphore
from collections import namedtuple
from datetime import timedelta
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
