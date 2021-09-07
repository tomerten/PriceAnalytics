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
from tqdm import tqdm

from .DataBroker import DataBrokerMongoDb
from .LoggingUtils import logger
from .ParseUtils import (
    generate_database_indices_dict,
    parse_prices,
    parse_raw_fmt,
    parse_to_multiindex,
)


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


async def aparse_yahoo_prices(
    sem: Semaphore, tup: Tuple[str, dict], session: ClientSession
) -> Tuple[
    Union[str, None],
    Union[pd.DataFrame, None],
    Union[pd.DataFrame, None],
    Union[pd.DataFrame, None],
]:
    """
    Method to get and clean the yahoo price data.

    Args:
        - sem (Semaphore): internal counter
            REF: https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore
        - tup (Tuple[str, dict]): (url, params)
        - session (ClientSession): aiohttp client session

    Returns:
        Tuple[ Union[str, None], Union[pd.DataFrame, None],
        Union[pd.DataFrame, None],Union[pd.DataFrame, None]]:
            parsed data as tuple (interval, pricedata, dividends, splits)
    """
    try:
        url, params = tup
        print(url, params)
        resp = await bound_fetch(sem, url, params, session)
        resp = resp["chart"]["result"][0]
        interval, pricedata, div, split = parse_prices(resp)

        logger.debug(colored(f"{url.split('/')[-1]:8} - interval {interval} - OK", "green"))

        return interval, pricedata, div, split

    except (ClientError, HttpProcessingError) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            tup[0],
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return None, None, None, None
    except Exception as e:
        logger.info(e)
        # logger.exception(
        #     "Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {})
        # )
        logger.exception(colored(f"{tup[0].split('/')[-1]}", "red"))
        return None, None, None, None


async def store_yahoo_prices(
    sem: Semaphore,
    tup: Tuple[str, dict],
    session: ClientSession,
    databroker: DataBrokerMongoDb,
    dbname: str = "FinData",
):
    """
    Method to get, clean and store (mongodb via DataBroker) the yahoo price data.

    Args:
        - sem: internal counter https://docs.python.org/3/library/asyncio-sync.html#asyncio.Semaphore
        - tup: (url, params)
        - session: aiohttp client session
        - databroker: DataBrokerMongoDb instance
        - dbname: name of the database to write the data to

    """
    # ASYNC GET AND PARSE DATA
    interval: Union[str, None] = None
    prices: Union[pd.DataFrame, None] = None
    div: Union[pd.DataFrame, None] = None
    split: Union[pd.DataFrame, None] = None
    interval, prices, div, split = await aparse_yahoo_prices(sem, tup, session)

    # SET DATABASE INDEX FOR THE DATA
    index: List[Tuple[str, int]] = [("symbol", ASCENDING), ("date", ASCENDING)]

    if interval is not None:
        if "h" in interval or ("m" in interval and "mo" not in interval):
            index = [("symbol", ASCENDING), ("datetime", ASCENDING)]

    if prices is not None:
        if not prices.empty:
            try:
                databroker.save(
                    prices.reset_index().to_dict(orient="records"),
                    dbname,
                    interval,
                    index,
                )
            except ValueError:
                logger.exception(
                    colored(
                        f"Failed saving prices for {tup[0].split('/')[-1]} - interval {interval}"
                    ),
                    "red",
                )

    indexdiv = [("symbol", ASCENDING), ("date", ASCENDING)]

    if div is not None:
        # STORE DIVIDENDS
        try:
            databroker.save(
                div.reset_index().to_dict(orient="records"),
                dbname,
                "Dividends",
                indexTupleList=indexdiv,
            )
        except ValueError:
            logger.exception(
                colored(
                    f"Failed saving dividends for {tup[0].split('/')[-1]} - interval {interval}"
                ),
                "red",
            )
    if split is not None:
        # STORE SPLITS
        try:
            databroker.save(
                split.reset_index().to_dict(orient="records"),
                dbname,
                "Splits",
                indexTupleList=indexdiv,
            )
        except ValueError:
            logger.exception(
                colored(f"Failed saving splits for {tup[0].split('/')[-1]} - interval {interval}"),
                "red",
            )

    logger.debug(colored(f'Saving {tup[0].split("/")[-1]:8} - {interval} done !', "green"))


async def aparse_raw_yahoo_financial_data(
    sem: Semaphore, tup: Tuple[str, dict], session: ClientSession
) -> dict:
    """Method to async get and parse yahoo raw financial data.

    Args:
        sem (Semaphore): semaphore
        tup (Tuple[str, dict]): (url, param])
        session (ClientSession): asynch ClientSession instance

    Returns:
        dict: key is data info and values are the actual data
    """
    try:
        url, params = tup
        resp = await bound_fetch(sem, url, params, session)
        resp = resp["quoteSummary"]["result"][0]
        cleaned_resp = parse_raw_fmt(resp)
        logger.debug(f"Cleaning done for {tup[0].split('/')[-1]}")
        return cleaned_resp

    except (
        ClientError,
        HttpProcessingError,
    ) as e:
        logger.error(
            "aiohttp exception for %s [%s]: %s",
            tup[0],
            getattr(e, "status", None),
            getattr(e, "message", None),
        )
        return {}
    except Exception as e:
        logger.exception("Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {}))
        return {}


async def aparse_multiindex_yahoo_financial_data(
    sem: Semaphore, tup: Tuple[str, dict], session: ClientSession
) -> Generator:
    """Method to async get and transform data into multi-index. Part of
    stage wise cleaning of the raw data.

    Args:
        sem (Semaphore): semaphore
        tup (Tuple[str, dict]): (url, params])
        session (ClientSession): asynch clientsession instance

    Returns:
        Generator: multi-index dict

    Yields:
        Generator: multi-index dict
    """
    try:
        data = await aparse_raw_yahoo_financial_data(sem, tup, session)
        transformed_financial_data = parse_to_multiindex(data)
        logger.debug(f"Multi-indexing done for {tup[0].split('/')[-1]}")
        return transformed_financial_data

    except Exception as e:
        logger.exception("Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {}))
        return ({} for i in range(0))


async def aparse_yahoo_financial_data(
    sem: Semaphore, tup: Tuple[str, dict], session: ClientSession
) -> dict:
    """Next step in the data cleaning process for financial data.

    Args:
        - sem (Semaphore): semphore
        - tup (Tuple[str, dict]): (url, params)
        - session (ClientSession): async ClientSession

    Returns:
        dict: data dict
    """
    # FINANCIAL DATA NOT CONTAINING SOME KIND
    # OF DATE REFERENCE NEED MANUAL ADDING OF DATE
    DATEUPDATELIST = [
        "assetProfile",
        "recommendationTrend_trend",
        "indexTrend_estimates",
        "indexTrend",
        "defaultKeyStatistics",
        "summaryDetail",
        "calendarEvents_earnings",
        "price",
        "earningsTrend_trend_earningsEstimate",
        "earningsTrend_trend_revenueEstimate",
        "earningsTrend_trend_epsTrend",
        "earningsTrend_trend_epsRevisions",
        "earningsTrend_trend",
        "majorHoldersBreakdown",
        "earningsHistory_history",
        "netSharePurchaseActivity",
        "insiderTransactions_transactions",
        "financialData",
        "quoteType",
        "calendarEvents",
        "esgScores_peerEsgScorePerformance",
        "esgScores_peerEnvironmentPerformance",
        "esgScores_peerEsgScorePerformance",
        "esgScores_peerGovernancePerformance",
        "esgScores_peerHighestControversyPerformance",
        "esgScores_peerSocialPerformance",
        "majorDirectHolders_holders",
    ]
    try:
        tf = await aparse_multiindex_yahoo_financial_data(sem, tup, session)
        tfd = list(tf)

        _keys = [k for k, g in itertools.groupby(tfd, lambda x: list(x.keys())[0])]
        cglist = [
            list(map(lambda d: list(d.values())[0], list(g)))
            for k, g in itertools.groupby(tfd, lambda x: list(x.keys())[0])
        ]
        dc = dict(zip(_keys, cglist))

        dc = {
            k: v for k, v in dc.items() if v not in [[None], [{"maxAge": 86400}], [{"maxAge": 1}]]
        }
        dfsdc = {"_".join(k): pd.DataFrame(v) for k, v in dc.items()}
        date = dt.fromtimestamp(time.mktime(dt.today().date().timetuple()))

        current_symbol = tup[0].split("/")[-1]

        for k, v in dfsdc.items():
            if k in DATEUPDATELIST:
                v["date"] = date.strftime("%Y-%m-%d")
                if k == "majorHoldersBreakdown":
                    v["reportDate"] = date.strftime("%Y-%m-%d")

            if k != "quoteType":
                v["symbol"] = current_symbol

        data = {k: v.to_dict(orient="records") for k, v in dfsdc.items()}

        logger.info(f"Processing financial data {current_symbol} - done!")

        return data

    except Exception as e:
        logger.exception("Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {}))
        return {}


async def store_yahoo_financial_data(
    sem: Semaphore,
    tup: Tuple[str, dict],
    session: ClientSession,
    databroker: DataBrokerMongoDb,
    dbname: str = "FinData",
):
    """Storing in database step of the data cleaning process.

    Args:
        - sem (Semaphore): semaphore
        - tup (Tuple[str, dict]): (url, params)
        - session (ClientSession): async ClientSession
        - databroker (DataBrokerMongoDb): MongoDb databroker instance
        - dbname (str, optional): Name of the database to store in. Defaults to "FinData".
    """
    findata = await aparse_yahoo_financial_data(sem, tup, session)
    indexdict = generate_database_indices_dict(findata)

    newindexdict = {}
    if isinstance(databroker, DataBrokerMongoDb):
        for k, v in indexdict.items():
            newindexdict[k] = [(vv, ASCENDING) for vv in v]
    else:
        newindexdict = indexdict.copy()

    # logger.info(f"Findata : {findata}")
    # logger.info(f"indexdict: {newindexdict}")

    for k, v in findata.items():
        # logger.info(k, newindexdict[k])
        # vv = deepcopy(v)  # otherwise the original dict (self._yh_finjson) is updated with _id field
        databroker.save(v, dbname, k, indexTupleList=newindexdict[k], unique=True)

    logger.info(colored(f'Saving {tup[0].split("/")[-1]} yahoo financials done !', "green"))
