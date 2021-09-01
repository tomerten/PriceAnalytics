# -*- coding: utf-8 -*-

"""
Module priceana.utils.AsyncUtils
=================================================================

A module containing methods for getting data asynchronuously.   

"""

import itertools
import time
from asyncio import Semaphore
from datetime import datetime as dt
from typing import Generator, List, Tuple, Union

import pandas as pd
from aiohttp import ClientError, ClientSession
from aiohttp.http import HttpProcessingError

# from FinDataBroker.DataBrokerMongoDb import DataBrokerMongoDb
from pymongo import ASCENDING
from termcolor import colored

from .LoggingUtils import logger


async def fetch(url: str, params: dict, session: ClientSession) -> dict:
    """
    Asynchronous fetching of urls.

    Args:
        - url (str): url to fetch
        - params (dict): parameters to pass to the request
        - session (ClientSession): aiohttp client session

    Returns:
        dict : json response from url
    """
    async with session.get(url, params=params) as response:
        # delay = response.headers.get("DELAY")
        # DISPLAY LOGGER MESSAGE GREEN IF FETCHING URL OK
        # IN RED IF FETCHING URL NOK
        # ONLY IN LOGGER DEBUG LEVEL
        if response.status == 200:
            color: str = "green"
            logger.debug(
                colored(
                    f"{url.split('/')[-1]:8} - {response.status} - {params.get('interval', '')} {params.get('t', '')}",
                    color,
                )
            )
        else:
            color = "red"
            logger.debug(
                colored(
                    f"{url.split('/')[-1]:8} - {response.status} - {params.get('interval', '')} {params.get('t', '')}",
                    color,
                )
            )
        json = await response.json()
        return json


async def bound_fetch(sem: Semaphore, url: str, params: dict, session: ClientSession) -> dict:
    """
    Method to restrict the open files (request) in async fetch.

    REF: https://pawelmhm.github.io/asyncio/python/aiohttp/2016/04/22/asyncio-aiohttp.html

    Args:
        - sem (Semaphore): internal counter
            REF: https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore
        - url (str): url to fetch
        - params (dict): parameters to pass to the request
        - session (ClientSession): aiohttp client session

    Returns:
        dict : json response from url
    """
    async with sem:
        return await fetch(url, params, session)
