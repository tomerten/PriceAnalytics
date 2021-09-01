# -*- coding: utf-8 -*-

"""
Module priceana.utils.ParseUtils
=================================================================

A module containing methods for parsing downloaded data.   

"""
import itertools
import time
from copy import deepcopy
from datetime import datetime as dt
from typing import Dict, Generator, List, Tuple, Union

import numpy as np
import pandas as pd

from .LoggingUtils import logger


def parse_quotes_as_frame(data: dict) -> pd.DataFrame:
    """
    Private method to parse raw yahoo price data
    into a dataframe.

    Args:
        - data (dict): raw yahoo json data

    Returns:
        pd.dataframe: dataframe version of the data
    """
    # GET INFO FROM THE METADATA
    try:
        symbol, exchange, currency, interval, priceHint = tuple(
            data.get("meta", {}).get(i)
            for i in [
                "symbol",
                "exchangeName",
                "currency",
                "dataGranularity",
                "priceHint",
            ]
        )

        date_list = data["timestamp"]
        ohlc_list = data["indicators"]["quote"][0]

        volume_list = ohlc_list["volume"]
        open_list = ohlc_list["open"]
        close_list = ohlc_list["close"]
        low_list = ohlc_list["low"]
        high_list = ohlc_list["high"]

        quotes = pd.DataFrame(
            {
                "open": open_list,
                "high": high_list,
                "low": low_list,
                "close": close_list,
                "volume": volume_list,
            }
        )
        if "adjclose" in data["indicators"]:
            adjclose_list = data["indicators"]["adjclose"][0]["adjclose"]
        else:
            adjclose_list = close_list

        quotes["adjclose"] = adjclose_list
        quotes["symbol"] = symbol
        quotes["currency"] = currency
        quotes["exchange"] = exchange

        quotes.index = pd.to_datetime(date_list, unit="s")
        quotes.sort_index(inplace=True)

        # ROUND ALL THE VALUES IN THE FRAME TO
        # FIXED NUMBER OF DECIMALS - EXTRACTED FROM
        # METADATA
        quotes = np.round(quotes, priceHint)

        # REFORMAT VOLUME AS INTEGERS - DATA REDUCTION
        quotes["volume"] = quotes["volume"].fillna(0).astype(np.int64)

        quotes.dropna(inplace=True)

        # SET QUOTES INDEX TO DATETIME USING LOCALIZATION !!!!
        quotes.index = quotes.index.tz_localize("UTC").tz_convert(
            data["meta"]["exchangeTimezoneName"]
        )

        if (interval[-1] == "m") or (interval[-1] == "h"):
            quotes.index = [ts.isoformat() for ts in quotes.index]
            quotes.index.name = "datetime"
        else:
            quotes.index = pd.to_datetime(quotes.index.date)
            quotes.index = [ts.strftime("%Y-%m-%d") for ts in quotes.index]
            quotes.index.name = "date"

        return quotes

    except (TypeError, AttributeError, KeyError, ValueError, IndexError) as e:
        # IF THERE ARE NO TIMESTAMPS RETURN EMPTY FRAME
        # SAME FOR IF THERE IS NO METADATA
        logger.info(f"Invalid data {e}")

        quotes = pd.DataFrame(columns=["open", "high", "low", "close", "adjclose", "volume"])
        return quotes


def _parse_actions_as_frame(
    data: dict,
) -> Union[Tuple[pd.DataFrame, pd.DataFrame], Tuple[None, None]]:
    """
    Private method to parse dividends and splits as dataframe.

    Args:
        - data (dict): raw yahoo action data

    Returns:
        tuple: (dividend, splits)
    """
    try:

        symbol, currency, priceHint = tuple(
            data.get("meta", {}).get(i) for i in ["symbol", "currency", "priceHint"]
        )
        divdc = data["events"].get("dividends", None)
        spldc = data["events"].get("splits", None)

    except (TypeError, AttributeError, ValueError, IndexError):
        # IF THERE ARE NO TIMESTAMPS RETURN EMPTY FRAME
        # SAME FOR IF THERE IS NO METADATA
        logger.info(f"Invalid data actions {data}")

        return None, None

    except KeyError:
        # There is no events key
        return None, None

    dividend = None
    split = None

    if divdc:
        try:
            dividend = pd.DataFrame(data=list(divdc.values()))
            dividend.set_index("date", inplace=True)
            dividend.index = pd.to_datetime(dividend.index, unit="s")
            dividend.sort_index(inplace=True)
            dividend.columns = ["dividends"]
            dividend.index = [ts.strftime("%Y-%m-%d") for ts in dividend.index]
            dividend.index.name = "date"
            dividend = np.round(dividend, priceHint)
            dividend["symbol"] = symbol
            dividend["currency"] = currency
        except (KeyError, TypeError, ValueError, AttributeError):
            dividend = None

    if spldc:
        try:
            split = pd.DataFrame(data=list(spldc.values()))
            split.set_index("date", inplace=True)
            split.index = pd.to_datetime(split.index, unit="s")
            split.sort_index(inplace=True)
            split["splits"] = split["numerator"] / split["denominator"]
            split.index = [ts.strftime("%Y-%m-%d") for ts in split.index]
            split.index.name = "date"
            split["symbol"] = symbol
        except (KeyError, TypeError, ValueError, AttributeError):
            split = None

    return dividend, split


def parse_raw_fmt(dc: Union[dict, None]) -> dict:
    """
    Private method to clean yahoo returned data. More specifically to remove the
    duplicate values in different formats.

    NOTE:
        This is a recursive method.

    Args:
        - dc(Union[dict, None]): input dictionary to clean

    Returns:
        dict: cleaned dictionary
    """
    # COPY ORIGINAL DICT AS DICT IS UPDATED DURING CLEANING
    if dc is None:
        return {}

    newdc = deepcopy(dc)

    for key, value in dc.items():
        if isinstance(value, dict):
            if "raw" in value.keys():
                # RAW EXCEPTION LIST -> USE FMT HERE
                _datelist = [
                    "date",
                    "lastfiscalYearEnd",
                    "nextfiscalYearEnd",
                    "mostrecentQuarter",
                ]
                if any(substring.lower() in key.lower() for substring in _datelist):
                    newdc[key] = value.get("fmt", None)

                # IF DATA IS IN PERCENTAGE USE FMT
                elif "percent" in key.lower():
                    newdc[key] = float(value.get("fmt", None).split("%")[0].replace(",", ""))
                else:
                    newdc[key] = float(value.get("raw", None))
            else:
                if value:
                    newdc[key] = parse_raw_fmt(dc[key])
        elif isinstance(value, list):
            newdc[key] = []
            for i, el in enumerate(value):
                if isinstance(el, dict):
                    newdc[key].append(parse_raw_fmt(dc[key][i]))

    return newdc


def parse_to_multiindex(v: Union[dict, None], prefix=tuple()) -> Generator:
    """
    Private method to transform the nested dictionaries into
    a flat dictionary with tuples as keys.

    NOTE:
        This is a recursive function.

    Args:
        - v (Union[dict, None]): dictionary to flatten
        - prefix (tuple): previous level key - can already be a tuple
            starting value is empty tuple

    Returns:
        Generator: flattened multi-index dictionary
    """
    # CHECK IF INPUT IS DICT
    if isinstance(v, dict):
        # IF DICT EMPTY DO NOTHING
        if v == {}:
            pass
        else:
            if all([isinstance(v2, (float, str, int)) for v2 in v.values()]):
                yield {prefix: v}
            elif any([isinstance(v2, (float, str, int)) for v2 in v.values()]):
                # split
                final = {}
                for k, v2 in v.items():
                    if isinstance(v2, (float, str, int)):
                        final[k] = v2
                    else:
                        p2 = prefix + (k,)
                        yield from parse_to_multiindex(v2, p2)
                if final:
                    yield from parse_to_multiindex(final, prefix)
            else:
                for k, v2 in v.items():
                    p2 = prefix + (k,)
                    yield from parse_to_multiindex(v2, p2)
    elif isinstance(v, list):
        for i, v2 in enumerate(v):
            p2 = prefix  # + (i,)
            yield from parse_to_multiindex(v2, p2)
    elif v is None:
        pass
    else:
        yield {prefix: v}


def parse_from_multiindex(current_symbol: str, gen: Generator) -> dict:
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
        "esgScores_peerEnvironmentPerformance",
        "esgScores_peerEsgScorePerformance",
        "esgScores_peerGovernancePerformance",
        "esgScores_peerHighestControversyPerformance",
        "esgScores_peerSocialPerformance",
        "majorHoldersBreakdown",
        "earningsHistory_history",
        "netSharePurchaseActivity",
        "insiderTransactions_transactions",
        "financialData",
        "quoteType",
        "calendarEvents",
    ]
    try:
        tfd = list(gen)

        _keys = [k for k, g in itertools.groupby(tfd, lambda x: list(x.keys())[0])]
        cglist = [
            list(map(lambda d: list(d.values())[0], list(g)))
            for k, g in itertools.groupby(tfd, lambda x: list(x.keys())[0])
        ]
        dc = dict(zip(_keys, cglist))
        dc = {
            k: v for k, v in dc.items() if v not in [[None], [{"maxAge": 86400}], [{"maxAge": 1}]]
        }
        dfsdc = {"_".join(k): pd.DataFrame(v).fillna(0.0) for k, v in dc.items()}
        date = dt.fromtimestamp(time.mktime(dt.today().date().timetuple()))

        # current_symbol = tup[0].split('/')[-1]

        for k, v in dfsdc.items():
            if k in DATEUPDATELIST:
                v["date"] = date.strftime("%Y-%m-%d")

            if k != "quoteType":
                v["symbol"] = current_symbol

        data = {k: v.to_dict(orient="records") for k, v in dfsdc.items()}

        logger.debug(f"Processing financial data {current_symbol} - done!")

        return data

    except Exception as e:
        logger.exception("Non-aiohttp exception occured:  %s", getattr(e, "__dict__", {}))
        return {}


def parse_prices(
    data: Union[dict, None]
) -> Tuple[
    Union[str, None],
    Union[pd.DataFrame, None],
    Union[pd.DataFrame, None],
    Union[pd.DataFrame, None],
]:
    """
    Private method to clean price/actions data.

    Args:
        - data (Union[dict, None]):  raw json data

    Returns:
        Tuple[Union[str, None], Union[pd.DataFrame, None],
        Union[pd.DataFrame, None], Union[pd.DataFrame, None]]:
            price time-series interval, prices, dividends and splits
    """
    if data is not None:
        meta = data.get("meta")
        if meta is not None:
            interval = meta.get("dataGranularity")
        else:
            interval = None

        quotes = parse_quotes_as_frame(data)
        dividends, splits = _parse_actions_as_frame(data)
        return interval, quotes, dividends, splits
    else:
        return None, None, None, None


# BELOW IS THE DATABASE INDEX GENERATOR RELATED METHODS
# THIS USES THE chain of responsibility DESIGN PATTERN
# MAKING THIS EASY TO EXTEND AND TO HANDLE THE DIFFERENT
# CASES IN A CLEAR FASHION.
# THIS APPROACH AVOIDS THE COMPLEX NESTED IF-ELSE STRUCTURES

indexDict: Dict[str, List[str]] = {
    "Dividends": ["symbol", "date"],
    "assetProfile": ["symbol", "date"],
    "assetProfile_companyOfficers": ["symbol", "fiscalYear", "name"],
    "balanceSheetHistoryQuarterly_balanceSheetStatements": ["symbol", "endDate"],
    "balanceSheetHistory_balanceSheetStatements": ["symbol", "endDate"],
    "calendarEvents": ["symbol", "date"],
    "calendarEvents_earnings": ["symbol", "date"],
    "calendarEvents_earnings_earningsDate": ["symbol", "fmt"],
    "cashflowStatementHistoryQuarterly_cashflowStatements": ["symbol", "endDate"],
    "cashflowStatementHistory_cashflowStatements": ["symbol", "endDate"],
    "defaultKeyStatistics": ["symbol", "date"],
    "earnings": ["symbol"],
    "earningsHistory_history": ["symbol", "date", "period"],
    "earningsTrend_trend": ["symbol", "date", "period"],
    "earningsTrend_trend_earningsEstimate": ["symbol", "date"],
    "earningsTrend_trend_epsRevisions": ["symbol", "date"],
    "earningsTrend_trend_epsTrend": ["symbol", "date"],
    "earningsTrend_trend_revenueEstimate": ["symbol", "date"],
    "earnings_earningsChart": [
        "symbol",
        "currentQuarterEstimateYear",
        "currentQuarterEstimateDate",
    ],
    "earnings_earningsChart_earningsDate": ["symbol", "fmt"],
    "earnings_earningsChart_quarterly": ["symbol", "date"],
    "earnings_financialsChart_quarterly": ["symbol", "date"],
    "earnings_financialsChart_yearly": ["symbol", "date"],
    "esgScores": ["symbol", "ratingYear", "ratingMonth"],
    "esgScores_peerEnvironmentPerformance": ["symbol", "date"],
    "esgScores_peerEsgScorePerformance": ["symbol", "date"],
    "esgScores_peerGovernancePerformance": ["symbol", "date"],
    "esgScores_peerHighestControversyPerformance": ["symbol", "date"],
    "esgScores_peerSocialPerformance": ["symbol", "date"],
    "financialData": ["symbol", "date"],
    "fundOwnership_ownershipList": ["symbol", "reportDate", "organization"],
    "incomeStatementHistoryQuarterly_incomeStatementHistory": ["symbol", "endDate"],
    "incomeStatementHistory_incomeStatementHistory": ["symbol", "endDate"],
    "indexTrend": ["symbol", "date"],
    "indexTrend_estimates": ["symbol", "date", "period"],
    "insiderHolders_holders": ["symbol", "positionDirectDate", "name"],
    "insiderTransactions_transactions": [
        "symbol",
        "startDate",
        "filerName",
        "ownership",
        "value",
        "shares",
        "transactionText",
    ],
    "institutionOwnership_ownershipList": ["symbol", "reportDate", "organization"],
    "majorHoldersBreakdown": ["symbol", "reportDate"],
    "majorDirectHolders_holders": ["symbol", "date"],
    "ms_balancesheet": ["symbol_yahoo", "year"],
    "ms_cashflowsheet": ["symbol_yahoo", "year"],
    "ms_financials": ["symbol_yahoo", "year"],
    "ms_liquiditysheet": ["symbol_yahoo", "year"],
    "ms_profitabilitysheet": ["symbol_yahoo", "year"],
    "netSharePurchaseActivity": ["symbol", "date"],
    "price": ["symbol", "date"],
    "quoteType": ["symbol", "date"],
    "recommendationTrend_trend": ["symbol", "date", "period"],
    "secFilings_filings": ["symbol", "date", "epochDate", "type"],
    "summaryDetail": ["symbol", "date"],
    "upgradeDowngradeHistory_history": [
        "epochGradeDate",
        "firm",
        "toGrade",
        "fromGrade",
        "action",
        "symbol",
    ],
}


def generate_database_indices_dict(dc: Union[dict, None]) -> dict:
    if dc == {} or dc is None:
        return {}
    else:
        requests = []
        requests_keys = []
        for k, vdc in dc.items():
            if vdc == [] or vdc is None:
                continue
            else:
                v = pd.DataFrame(vdc)
                requests_keys.append(k)
                requests.append(v)

        # db_index_handler_client = DatabaseIndexHandlerClient()
        # res = db_index_handler_client.delegate(requests)

        res = dict(zip(requests_keys, [indexDict[k] for k in requests_keys]))
        return res  # dict(zip(requests_keys, res))
