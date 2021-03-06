#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `priceana` package."""
import itertools
import time
from asyncio import Semaphore
from datetime import datetime as dt

import mongomock
import numpy as np
import pandas as pd
import pytest
from aiohttp import ClientSession
from asynctest import CoroutineMock, patch
from pandas.testing import assert_frame_equal
from priceana.constants import base_url, query_url
from priceana.utils.AsyncUtils import (
    aparse_multiindex_yahoo_financial_data,
    aparse_raw_yahoo_financial_data,
    aparse_yahoo_financial_data,
    aparse_yahoo_prices,
    bound_fetch,
    fetch,
    store_yahoo_financial_data,
    store_yahoo_prices,
)
from priceana.utils.DataBroker import DataBrokerMongoDb
from priceana.utils.DateTimeUtils import clean_start_end_period, validate_date
from priceana.utils.ParseUtils import (
    generate_database_indices_dict,
    parse_from_multiindex,
    parse_prices,
    parse_quotes_as_frame,
    parse_raw_fmt,
    parse_to_multiindex,
)
from priceana.utils.UrlUtils import (
    generate_combinations,
    generate_price_params,
    generate_price_urls,
    generate_yahoo_financial_data_params,
    generate_yahoo_financial_data_urls,
)
from pytest import raises


# PATCH TIME TO GET FIXED NOW (time.time())
@pytest.fixture()
def patchtime(monkeypatch):
    def mytime():
        return 10

    monkeypatch.setattr(time, "time", mytime)


# PATCH TIME MKTIME TO GET FIXED VALUE (time.mktime())
@pytest.fixture()
def patchmktime(monkeypatch):
    def mktime(val):
        return 100

    monkeypatch.setattr(time, "mktime", mktime)


# MOCKING OF MONGODB FOR TESTING
@pytest.fixture
def databroker():
    client = mongomock.MongoClient()
    broker = DataBrokerMongoDb(client)
    return broker


################################################################################
# TESTS FOR URLUTILS
################################################################################

all_interval = [
    {"range": "5d", "includePrePost": True, "events": "div,splits", "interval": "1m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "2m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "5m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "15m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "30m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "90m"},
    {"range": "1mo", "includePrePost": True, "events": "div,splits", "interval": "1h"},
    {
        "period1": 0,
        "period2": 10,
        "includePrePost": True,
        "events": "div,splits",
        "interval": "1d",
    },
    {
        "period1": 0,
        "period2": 10,
        "includePrePost": True,
        "events": "div,splits",
        "interval": "5d",
    },
    {
        "period1": 0,
        "period2": 10,
        "includePrePost": True,
        "events": "div,splits",
        "interval": "1wk",
    },
    {
        "period1": 0,
        "period2": 10,
        "includePrePost": True,
        "events": "div,splits",
        "interval": "1mo",
    },
    {
        "period1": 0,
        "period2": 10,
        "includePrePost": True,
        "events": "div,splits",
        "interval": "3mo",
    },
]

test_price_urls = [
    ([], []),
    (["abc"], [f"{base_url}chart/{'abc'}"]),
    (["a", "b"], [f"{base_url}chart/{'a'}", f"{base_url}chart/{'b'}"]),
]

test_price_params_fail: list = [
    (None, None, None, None),
    (None, None, "a", None),
    (None, None, None, "b"),
    (None, None, None, {}),
    (None, None, {}, None),
    (None, None, None, "01-01-2020"),
    (None, None, "01-01-2020", None),
    ("a", None, None, None),
    ("a", "1m", None, None),
]

test_price_params_pass = [
    (
        "a",
        "b",
        None,
        None,
        [{"range": "a", "includePrePost": 1, "events": "div,splits", "interval": "b"}],
    ),
    (
        "1d",
        "b",
        None,
        None,
        [{"range": "1d", "includePrePost": 1, "events": "div,splits", "interval": "b"}],
    ),
    (
        "a",
        "1d",
        None,
        None,
        [{"range": "a", "includePrePost": 1, "events": "div,splits", "interval": "1d"}],
    ),
    (
        "max",
        "1m",
        None,
        None,
        [
            {
                "range": "5d",
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1m",
            }
        ],
    ),
    (
        "1d",
        "1m",
        None,
        None,
        [
            {
                "range": "1d",
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1m",
            }
        ],
    ),
    (
        "1d",
        "1h",
        None,
        None,
        [
            {
                "range": "1d",
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1h",
            }
        ],
    ),
    (
        "max",
        "1h",
        None,
        None,
        [
            {
                "range": "1mo",
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1h",
            }
        ],
    ),
    (
        None,
        "1d",
        None,
        None,
        [
            {
                "period1": 0,
                "period2": 10,
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1d",
            }
        ],
    ),
    (
        "max",
        "1d",
        None,
        None,
        [
            {
                "period1": 0,
                "period2": 10,
                "includePrePost": 1,
                "events": "div,splits",
                "interval": "1d",
            }
        ],
    ),
    (
        "1d",
        "all",
        None,
        None,
        [
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "1m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "2m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "5m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "15m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "30m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "90m",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "1h",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "1d",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "5d",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "1wk",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "1mo",
            },
            {
                "range": "1d",
                "includePrePost": True,
                "events": "div,splits",
                "interval": "3mo",
            },
        ],
    ),
]

test_financial_url_fail = [("abc",), ({},), (1,), (None,), ([],), (3.0,)]

test_financial_url_pass = [
    (["a"], [f"{query_url}{'a'}"]),
    (["a", "b"], [f"{query_url}{'a'}", f"{query_url}{'b'}"]),
]

test_financial_params_fail = [("abc",), ({},), (1,), (None,), ([],), (3.0,)]

test_financial_params_pass = [
    (["a"], [{"modules": ",".join(["a"])}]),
    (["a", "b"], [{"modules": ",".join(["a", "b"])}]),
]


@pytest.mark.parametrize("symbollist,expected", test_price_urls)
def test___generate_price_url__pass(symbollist, expected):
    assert generate_price_urls(symbollist) == expected


def test___generate_price_url_none___fail():
    with raises(TypeError):
        generate_price_urls()


@pytest.mark.parametrize("period,interval,start,end", test_price_params_fail)
def test___generate_price_params___fail(patchtime, period, interval, start, end):
    with raises((TypeError, ValueError)):
        generate_price_params(period, interval, start, end)


@pytest.mark.parametrize("period,interval,start,end,expected", test_price_params_pass)
def test___generate_price_params___pass(patchtime, period, interval, start, end, expected):
    assert expected == generate_price_params(period, interval, start, end)


@pytest.mark.parametrize("symbols", test_financial_url_fail)
def test___generate_findata_url___fail(patchtime, symbols):
    with raises(TypeError):
        generate_yahoo_financial_data_urls(symbols)


@pytest.mark.parametrize("symbols, expected", test_financial_url_pass)
def test___generate_findata_url___pass(patchtime, symbols, expected):
    assert expected == generate_yahoo_financial_data_urls(symbols)


@pytest.mark.parametrize("keys", test_financial_params_fail)
def test___generate_findata_params___fail(patchtime, keys):
    with raises(TypeError):
        generate_yahoo_financial_data_params(keys)


@pytest.mark.parametrize("keys, expected", test_financial_params_pass)
def test___generate_findata_params___pass(patchtime, keys, expected):
    assert expected == generate_yahoo_financial_data_params(keys)


def test__generate_combinations___pass():
    symbols = ["a"]
    urls = generate_yahoo_financial_data_urls(symbols)
    params = generate_yahoo_financial_data_params(["b"])
    expected_urls = [f"{query_url}{'a'}"]
    expected_params = [{"modules": ",".join(["b"])}]
    expected = list(itertools.product(expected_urls, expected_params))
    actual = generate_combinations(urls, params)
    assert expected == actual


################################################################################
# TESTS FOR DATETIMEUTILS
################################################################################
def test__clean_start_end_period___fail():
    with raises(TypeError):
        clean_start_end_period()


def test__clean_start_end_period___pass(patchtime):
    params = clean_start_end_period(None, None, None)
    assert params == {"period1": 0, "period2": 10}


def test__clean_start_end_period___period___pass(patchtime):
    params = clean_start_end_period(None, None, period="1d")
    assert params == {"range": "1d"}


def test__clean_start_end_period___end___fail(patchtime):
    with raises(ValueError):
        clean_start_end_period(None, 5, None)


def test__clean_start_end_period___end___pass(patchtime, patchmktime):
    params = clean_start_end_period(None, "2020-01-02", None)
    assert params == {"period1": 0, "period2": 100}


def test__clean_start_end_period___enddt___pass(patchtime, patchmktime):
    params = clean_start_end_period(None, dt(2020, 1, 2), None)
    assert params == {"period1": 0, "period2": 100}


def test__clean_start_end_period___endtime___pass(patchtime, patchmktime):
    params = clean_start_end_period(None, dt(2020, 1, 2, 0, 0, 10), None)
    assert params == {"period1": 0, "period2": 100}


def test__clean_start_end_period___start___pass(patchtime, patchmktime):
    params = clean_start_end_period("2020-01-02", None, None)
    assert params == {"period1": 100, "period2": 10}


def test__clean_start_end_period___startdt___pass(patchtime, patchmktime):
    params = clean_start_end_period(dt(2020, 1, 2), None, None)
    assert params == {"period1": 100, "period2": 10}


def test__clean_start_end_period___starttime___pass(patchtime, patchmktime):
    params = clean_start_end_period(dt(2020, 1, 2, 0, 0, 10), None, None)
    assert params == {"period1": 100, "period2": 10}


def test____validate_date___dt___pass():
    validate_date(dt(2020, 1, 1))


def test____validate_date___str___pass():
    validate_date("2012-02-01")


def test___validate_date___fail():
    with raises(ValueError):
        validate_date("boe")


def test___validate_date___typeerror___fail():
    with raises(TypeError):
        validate_date({})


################################################################################
# TESTS FOR PARSEUTILS
################################################################################


test_parse_raw_fmt_pass = [
    ({"test": {"raw": 100, "fmt": "1k"}}, {"test": 100.0}),
    ({"date": {"raw": 100, "fmt": "2020-01-01"}}, {"date": "2020-01-01"}),
    (
        {"lastfiscalYearEnd": {"raw": 100, "fmt": "2020-01-01"}},
        {"lastfiscalYearEnd": "2020-01-01"},
    ),
    (
        {"nextfiscalYearEnd": {"raw": 100, "fmt": "2020-01-01"}},
        {"nextfiscalYearEnd": "2020-01-01"},
    ),
    (
        {"mostrecentQuarter": {"raw": 100, "fmt": "2020-01-01"}},
        {"mostrecentQuarter": "2020-01-01"},
    ),
    ({"testpercent": {"raw": 100, "fmt": "100%"}}, {"testpercent": 100.0}),
    ({"nexttest": {"test": {"raw": 100, "fmt": "1k"}}}, {"nexttest": {"test": 100.0}}),
    (
        {"nexttest": [{"test": {"raw": 100, "fmt": "1k"}}]},
        {"nexttest": [{"test": 100.0}]},
    ),
    (
        {
            "nexttest": [
                {"test": {"raw": 100, "fmt": "1k"}},
                {"test2": {"raw": 10, "fmt": "1k"}},
            ]
        },
        {"nexttest": [{"test": 100.0}, {"test2": 10.0}]},
    ),
    ({}, {}),
    (None, {}),
    ({"test": 1}, {"test": 1}),
    (
        {"nexttest": [{"test": 1}, {"test2": {"raw": 1000, "fmt": "1k"}}]},
        {"nexttest": [{"test": 1}, {"test2": 1000.0}]},
    ),
]

test_parse_multiindex_pass = [
    ({}, [], {}),
    (
        {"test": 100, "test2": 1},
        [{(): {"test": 100, "test2": 1}}],
        {"": [{"test": 100, "test2": 1, "symbol": "abc"}]},
    ),
    (
        {"test": 100, "test2": {"dat0": 1, "dat1": 2}},
        [{("test2",): {"dat0": 1, "dat1": 2}}, {(): {"test": 100}}],
        {
            "test2": [{"dat0": 1, "dat1": 2, "symbol": "abc"}],
            "": [{"test": 100, "symbol": "abc"}],
        },
    ),
    (
        {"test": {"data": 100}},
        [{("test",): {"data": 100.0}}],
        {"test": [{"data": 100, "symbol": "abc"}]},
    ),
    (
        {"test2": {"test": {"data": 100}}},
        [{("test2", "test"): {"data": 100.0}}],
        {"test2_test": [{"data": 100, "symbol": "abc"}]},
    ),
    (None, [], {}),
    (
        [1, 2],
        [{(): 1}, {(): 2}],
        {"": [{0: 1, "symbol": "abc"}, {0: 2, "symbol": "abc"}]},
    ),
    (
        [{"a": 1}, {"b": 2}],
        [{(): {"a": 1}}, {(): {"b": 2}}],
        {
            "": [
                {"a": 1.0, "b": 0.0, "symbol": "abc"},
                {"a": 0.0, "b": 2.0, "symbol": "abc"},
            ]
        },
    ),
    (
        {"test": [1, 2]},
        [{("test",): 1}, {("test",): 2}],
        {"test": [{0: 1, "symbol": "abc"}, {0: 2, "symbol": "abc"}]},
    ),
    (
        {"test": [{"a": 1}, {"b": 2}]},
        [{("test",): {"a": 1}}, {("test",): {"b": 2}}],
        {
            "test": [
                {"a": 1.0, "b": 0.0, "symbol": "abc"},
                {"a": 0.0, "b": 2.0, "symbol": "abc"},
            ]
        },
    ),
]

empty_quote_frame = pd.DataFrame(columns=["open", "high", "low", "close", "adjclose", "volume"])
test_parse_prices_pass: list = [
    (None, None, None, None, None),
    ({}, None, empty_quote_frame, None, None),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            },
            "timestamp": [],
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            },
            "timestamp": [1, 2],
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            },
            "timestamp": [1, 2],
            "indicators": {"quote": None},
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            },
            "timestamp": [1, 2],
            "indicators": {"quote": {}},
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "meta": {
                "symbol": "abc",
                "exchangeName": "F",
                "priceHint": 2,
                "dataGranularity": "1d",
                "currency": "USD",
            },
            "timestamp": [1, 2],
            "indicators": {"quote": 12},
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
]

test_generate_database_indices_dict___pass: list = [
    (None, {}),
    ({}, {}),
    ({"test": None}, {}),
    ({"test": []}, {}),
    (
        {"quoteType": [{"symbol": "abc", "date": "2020-01-01", "val": 1}]},
        {"quoteType": ["symbol", "date"]},
    ),
    (
        {
            "quoteType": [
                {"symbol": "abc", "date": "2020-01-01", "val": 1},
                {"symbol": "abc", "date": "2020-01-02", "val": 1},
                {"symbol": "abc", "date": "2020-01-03", "val": 1},
            ]
        },
        {"quoteType": ["symbol", "date"]},
    ),
    (
        {
            "balanceSheetHistoryQuarterly_balanceSheetStatements": [
                {"symbol": "abc", "endDate": "2020-01-01", "val": 1},
                {"symbol": "abc", "endDate": "2020-01-02", "val": 1},
                {"symbol": "abc", "endDate": "2020-01-03", "val": 1},
            ]
        },
        {"balanceSheetHistoryQuarterly_balanceSheetStatements": ["symbol", "endDate"]},
    ),
    (
        {
            "assetProfile_companyOfficers": [
                {"symbol": "abc", "fiscalYear": "2020", "name": "X", "val": 1},
                {"symbol": "abc", "fiscalYear": "2020", "name": "Y", "val": 1},
                {"symbol": "abc", "fiscalYear": "2020", "name": "Z", "val": 1},
            ]
        },
        {"assetProfile_companyOfficers": ["symbol", "fiscalYear", "name"]},
    ),
    (
        {
            "fundOwnership_ownershipList": [
                {
                    "symbol": "abc",
                    "reportDate": "2020-01-01",
                    "organization": "org1",
                    "val": 1,
                },
                {
                    "symbol": "abc",
                    "reportDate": "2020-01-02",
                    "organization": "org1",
                    "val": 1,
                },
                {
                    "symbol": "abc",
                    "reportDate": "2020-01-03",
                    "organization": "org2",
                    "val": 1,
                },
            ]
        },
        {"fundOwnership_ownershipList": ["symbol", "reportDate", "organization"]},
    ),
]


@pytest.mark.parametrize("datadc,expected", test_parse_raw_fmt_pass)
def test___parse_raw_fmt___pass(datadc, expected):
    assert expected == parse_raw_fmt(datadc)


@pytest.mark.parametrize("dc,expected,expected2", test_parse_multiindex_pass)
def test___parse_to_multiindex___pass(dc, expected, expected2):
    assert expected == list(parse_to_multiindex(dc))


@pytest.mark.parametrize("dc,interval,quotes,div,split", test_parse_prices_pass)
def test___parse_prices___pass(dc, interval, quotes, div, split):
    i, q, d, s = parse_prices(dc)

    assert interval == i
    if q is not None:
        assert_frame_equal(q, quotes)
    else:
        assert q is None
    assert div == d
    assert split == s


@pytest.mark.parametrize("dc,expected, expected2", test_parse_multiindex_pass)
def test___parse_from_multiindex___pass(dc, expected, expected2):
    assert expected == list(parse_to_multiindex(dc))
    print(parse_from_multiindex("abc", parse_to_multiindex(dc)))
    actual = parse_from_multiindex("abc", parse_to_multiindex(dc))
    if "test" in actual.keys() and "b" in actual["test"][0].keys():
        print(type(actual["test"][0]["b"]))
        print(type(expected2["test"][0]["b"]))
        print((actual["test"][0]["b"]))
        print((expected2["test"][0]["b"]))
        print(np.isnan(expected2["test"][0]["b"]))
    assert expected2 == parse_from_multiindex("abc", parse_to_multiindex(dc))


@pytest.mark.parametrize("dc,expected", test_generate_database_indices_dict___pass)
def test___generate_database_indices_dict___pass(dc, expected):
    assert expected == generate_database_indices_dict(dc)


################################################################################
# TESTS FOR ASYNCUTILS
################################################################################

test_fetch_pass = [([{"a": 1}],), ([{"a": 1, "b": 2}],)]

empty_quote_frame = pd.DataFrame(columns=["open", "high", "low", "close", "adjclose", "volume"])
pricedc = {
    "meta": {
        "symbol": "abc",
        "exchangeName": "F",
        "priceHint": 2,
        "dataGranularity": "1d",
        "currency": "USD",
        "exchangeTimezoneName": "America/New_York",
    },
    "timestamp": [1583038800, 1585339201],
    "indicators": {
        "quote": [
            {
                "volume": [1478726800, 51054153],
                "close": [247.74000549316406, 247.74000549316406],
                "open": [282.2799987792969, 252.75],
                "high": [304.0, 255.8699951171875],
                "low": [212.61000061035156, 247.0500030517578],
            }
        ],
        "adjclose": [{"adjclose": [247.74000549316406, 247.74000549316406]}],
    },
}
priceframe = parse_quotes_as_frame(pricedc)

test_parse_prices_pass: list = [
    (None, None, None, None, None),
    ({"chart": {"result": [{}]}}, None, empty_quote_frame, None, None),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        }
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [],
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [1, 2],
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [1, 2],
                        "indicators": {"quote": None},
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [1, 2],
                        "indicators": {"quote": {}},
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [1, 2],
                        "indicators": {"quote": 12},
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "symbol": "abc",
                            "exchangeName": "F",
                            "priceHint": 2,
                            "dataGranularity": "1d",
                            "currency": "USD",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [1583038800, 1585339201],
                        "indicators": {
                            "quote": [
                                {
                                    "volume": [1478726800, 51054153],
                                    "close": [247.74000549316406, 247.74000549316406],
                                    "open": [282.2799987792969, 252.75],
                                    "high": [304.0, 255.8699951171875],
                                    "low": [212.61000061035156, 247.0500030517578],
                                }
                            ],
                            "adjclose": [{"adjclose": [247.74000549316406, 247.74000549316406]}],
                        },
                    }
                ]
            }
        },
        "1d",
        priceframe,
        None,
        None,
    ),
    (
        {
            "chart": {
                "result": [
                    {
                        "meta": {
                            "currency": None,
                            "symbol": "AET",
                            "exchangeName": "YHD",
                            "instrumentType": "MUTUALFUND",
                            "firstTradeDate": 1340006400,
                            "regularMarketTime": 1561759658,
                            "gmtoffset": -14400,
                            "timezone": "EDT",
                            "exchangeTimezoneName": "America/New_York",
                            "priceHint": 2,
                            "currentTradingPeriod": {
                                "pre": {
                                    "timezone": "EDT",
                                    "start": 1586419200,
                                    "end": 1586439000,
                                    "gmtoffset": -14400,
                                },
                                "regular": {
                                    "timezone": "EDT",
                                    "start": 1586439000,
                                    "end": 1586462400,
                                    "gmtoffset": -14400,
                                },
                                "post": {
                                    "timezone": "EDT",
                                    "start": 1586462400,
                                    "end": 1586476800,
                                    "gmtoffset": -14400,
                                },
                            },
                            "dataGranularity": "1d",
                            "range": "5d",
                            "validRanges": [
                                "1mo",
                                "3mo",
                                "6mo",
                                "ytd",
                                "1y",
                                "2y",
                                "5y",
                                "10y",
                                "max",
                            ],
                        },
                        "indicators": {"quote": [{}], "adjclose": [{}]},
                    }
                ]
            }
        },
        "1d",
        empty_quote_frame,
        None,
        None,
    ),
]

test_aparse_raw_yahoo_financial_data_pass: list = [
    ({}, {}),
    (None, {}),
    (
        {"quoteSummary": {"result": [{"test": {"raw": 100, "fmt": "1k"}}]}},
        {"test": 100.0},
    ),
    (
        {"quoteSummary": {"result": [{"date": {"raw": 100, "fmt": "2020-01-01"}}]}},
        {"date": "2020-01-01"},
    ),
    (
        {"quoteSummary": {"result": [{"lastfiscalYearEnd": {"raw": 100, "fmt": "2020-01-01"}}]}},
        {"lastfiscalYearEnd": "2020-01-01"},
    ),
    (
        {"quoteSummary": {"result": [{"nextfiscalYearEnd": {"raw": 100, "fmt": "2020-01-01"}}]}},
        {"nextfiscalYearEnd": "2020-01-01"},
    ),
    (
        {"quoteSummary": {"result": [{"mostrecentQuarter": {"raw": 100, "fmt": "2020-01-01"}}]}},
        {"mostrecentQuarter": "2020-01-01"},
    ),
    (
        {"quoteSummary": {"result": [{"testpercent": {"raw": 100, "fmt": "100%"}}]}},
        {"testpercent": 100.0},
    ),
    (
        {"quoteSummary": {"result": [{"nexttest": {"test": {"raw": 100, "fmt": "1k"}}}]}},
        {"nexttest": {"test": 100.0}},
    ),
    (
        {"quoteSummary": {"result": [{"nexttest": [{"test": {"raw": 100, "fmt": "1k"}}]}]}},
        {"nexttest": [{"test": 100.0}]},
    ),
]

test_aparse_raw_yahoo_financial_data_fail: list = [(None, {}), ({}, {})]

test_aparse_transform_pass: list = [
    ({}, []),
    (None, []),
    (
        {
            "quoteSummary": {
                "result": [{"test": {"raw": 100, "fmt": "1k"}, "test2": {"raw": 1, "fmt": "1"}}]
            }
        },
        [{(): {"test": 100, "test2": 1}}],
    ),
    (
        {"quoteSummary": {"result": [{"test": 100, "test2": {"dat0": 1, "dat1": 2}}]}},
        [{("test2",): {"dat0": 1, "dat1": 2}}, {(): {"test": 100}}],
    ),
    (
        {"quoteSummary": {"result": [{"test": {"data": 100}}]}},
        [{("test",): {"data": 100.0}}],
    ),
    (
        {"quoteSummary": {"result": [{"test2": {"test": {"data": 100}}}]}},
        [{("test2", "test"): {"data": 100.0}}],
    ),
    ({"quoteSummary": {"result": [[{"a": 1}, {"b": 2}]]}}, []),
]

test_aparse_yahoo_financial_data_pass: list = [
    ({}, {}),
    (None, {}),
    (
        {
            "quoteSummary": {
                "result": [{"test": {"raw": 100, "fmt": "1k"}, "test2": {"raw": 1, "fmt": "1"}}]
            }
        },
        {"": [{"test": 100.0, "test2": 1.0, "symbol": "example.com"}]},
    ),
    (
        {"quoteSummary": {"result": [{"test": 100, "test2": {"dat0": 1, "dat1": 2}}]}},
        {
            "test2": [{"dat0": 1, "dat1": 2, "symbol": "example.com"}],
            "": [{"test": 100, "symbol": "example.com"}],
        },
    ),
    (
        {"quoteSummary": {"result": [{"test": {"data": 100}}]}},
        {"test": [{"data": 100, "symbol": "example.com"}]},
    ),
    (
        {"quoteSummary": {"result": [{"test2": {"test": {"data": 100}}}]}},
        {"test2_test": [{"data": 100.0, "symbol": "example.com"}]},
    ),
    ({"quoteSummary": {"result": [[{"a": 1}, {"b": 2}]]}}, {}),
]


@pytest.mark.parametrize("expected", test_fetch_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___fetch__pass(mock_get, expected):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=expected)

    async with ClientSession() as session:
        data = await fetch("http://example.com", {}, session)

    assert data == expected[0]


@pytest.mark.parametrize("expected", test_fetch_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___bound_fetch___pass(mock_get, expected):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=expected)
    sem = Semaphore()
    async with ClientSession() as session:
        data = await bound_fetch(sem, "http://example.com", {}, session)

    assert data == expected[0]


@pytest.mark.parametrize("dc,interval,quotes,div,split", test_parse_prices_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___aparse_yahoo_prices___pass(mock_get, dc, interval, quotes, div, split):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        res = await aparse_yahoo_prices(sem, ("http://example.com", {}), session)

    if res is not None:
        i, q, d, s = res
        print(q)
        assert interval == i
        if q is not None:
            assert_frame_equal(q, quotes)
        else:
            assert q is None
        assert div == d
        assert split == s
    else:
        assert res is None


@pytest.mark.parametrize("dc,interval,quotes,div,split", [test_parse_prices_pass[-2]])
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___store_yahoo_prices___pass(mock_get, dc, interval, quotes, div, split, databroker):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        await store_yahoo_prices(
            sem,
            ("http://example.com", {}),
            session,
            databroker=databroker,
            dbname="FinDataTest",
        )

    assert databroker.client.list_database_names() == ["FinDataTest"]
    assert databroker.client["FinDataTest"].list_collection_names() == ["1d"]

    # clean up
    databroker.client.drop_database("FinDataTest")


@pytest.mark.parametrize("dc, expeceted", test_aparse_raw_yahoo_financial_data_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___aparse_raw_yahoo_financial_data___pass(mock_get, dc, expeceted):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        res = await aparse_raw_yahoo_financial_data(sem, ("http://example.com", {}), session)

    assert res == expeceted


@pytest.mark.parametrize("dc, expected", test_aparse_raw_yahoo_financial_data_fail)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___aparse_raw_yahoo_financial_data___fail(mock_get, dc, expected):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        res = await aparse_raw_yahoo_financial_data(sem, ("http://example.com", {}), session)

    assert res == {}


@pytest.mark.parametrize("dc, expected", test_aparse_transform_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___aparse_multiindex_yahoo_financial_data___pass(mock_get, dc, expected):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        res = await aparse_multiindex_yahoo_financial_data(
            sem, ("http://example.com", {}), session
        )

    assert list(res) == expected


@pytest.mark.parametrize("dc, expected", test_aparse_yahoo_financial_data_pass)
@pytest.mark.asyncio
@patch("aiohttp.ClientSession.get")
async def test___aparse_yahoo_financial_data___pass(mock_get, dc, expected):
    mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
    sem = Semaphore()
    async with ClientSession() as session:
        res = await aparse_yahoo_financial_data(sem, ("http://example.com", {}), session)

    print(res)
    assert res == expected


# @pytest.mark.parametrize("dc", test_store_yahoo_financial_data_pass)
# @pytest.mark.asyncio
# @patch("aiohttp.ClientSession.get")
# async def test___store_yahoo_financial_data___pass(mock_get, dc, databroker):
#     mock_get.return_value.__aenter__.return_value.json = CoroutineMock(side_effect=[dc])
#     sem = Semaphore()
#     async with ClientSession() as session:
#         # res = await aparse_yahoo_financial_data(sem, ('http://example.com', {}), session)
#         await store_yahoo_financial_data(
#             sem,
#             ("http://example.com", {}),
#             session,
#             databroker=databroker,
#             dbname="FinDataTest",
#         )
#
#     # print(res)
#     assert databroker.client.list_database_names() == ["FinDataTest"]
#     assert databroker.client["FinDataTest"].list_collection_names() == ["test"]
#
#     # clean up
#     databroker.client.drop_database("FinDataTest")


# ==============================================================================
# The code below is for debugging a particular test in eclipse/pydev.
# (normally all tests are run with pytest)
# ==============================================================================
if __name__ == "__main__":
    the_test_you_want_to_debug = test___generate_findata_params___fail

    the_test_you_want_to_debug()
    print("-*# finished #*-")
# ==============================================================================
