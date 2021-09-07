# -*- coding: utf-8 -*-

"""Tests for priceana package."""

import mongomock
import pytest
from priceana import InvalidIntervalError, InvalidPeriodError, YahooPrices
from priceana.utils.DataBroker import DataBrokerMongoDb
from pytest import raises


@pytest.fixture
def databroker():
    client = mongomock.MongoClient()
    broker = DataBrokerMongoDb(client)
    return broker


@pytest.fixture
def client():
    client = mongomock.MongoClient()
    return client


test_input_validation_fail = [
    ({}, TypeError, None),
    ({"tickers": "XYZ", "databroker": client}, TypeError, None),
    (
        {
            "tickers": "XYZ",
            "databroker": databroker,
            "start": [1, 2],
        },
        TypeError,
        None,
    ),
    (
        {"tickers": ["XYZ"], "period": "7d"},
        InvalidPeriodError,
        None,
    ),
    (
        {"tickers": ["XYZ"], "period": "45h"},
        InvalidPeriodError,
        None,
    ),
    (
        {"tickers": ["XYZ"], "start": "2020-20-01"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "start": "20-02-2020"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "start": "2020-01-01 abc"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "start": "2020-02-02 26:12:34"},
        ValueError,
        "Incorrect date str format",
    ),
    ({"tickers": ["XYZ"], "start": [1, 2]}, TypeError, None),
    (
        {"tickers": ["XYZ"], "end": "45h"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "end": "2020-20-01"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "end": "20-02-2020"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "end": "2020-01-01 abc"},
        ValueError,
        "Incorrect date str format",
    ),
    (
        {"tickers": ["XYZ"], "end": "2020-02-02 26:12:34"},
        ValueError,
        "Incorrect date str format",
    ),
    ({"tickers": ["XYZ"], "end": [1, 2]}, TypeError, None),
]

test_input_validation_pass = [
    {"tickers": ["XYZ"]},
]

test_input_validation_obj_pass = [
    ({"tickers": ["XYZ"], "start": "2020-01-01"}, ["XYZ"]),
    ({"tickers": ["XYZ"], "end": "2020-01-01"}, ["XYZ"]),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("ikwargs, err, msg", test_input_validation_fail)
async def test___input_validation_fail(ikwargs, err, msg, databroker, client):
    with raises(err) as error:
        YahooPrices(databroker=databroker, **ikwargs)

    if msg is not None:
        if err not in [ValueError, TypeError]:
            assert error.value.message == msg
        else:
            assert str(error.value) == msg


@pytest.mark.asyncio
@pytest.mark.parametrize("ikwargs", test_input_validation_pass)
async def test___input_validation_pass(ikwargs, databroker):
    YahooPrices(databroker=databroker, **ikwargs)


@pytest.mark.asyncio
@pytest.mark.parametrize("ikwargs, res", test_input_validation_obj_pass)
async def test___input_validation_obj_pass(ikwargs, res, databroker):
    pa = YahooPrices(databroker=databroker, **ikwargs)
    assert pa._tickers == res


# ==============================================================================
# The code below is for debugging a particular test in eclipse/pydev.
# (otherwise all tests are normally run with pytest)
# Make sure that you run this code with the project directory as CWD, and
# that the source directory is on the path
# ==============================================================================
if __name__ == "__main__":
    the_test_you_want_to_debug = None

    print("__main__ running", the_test_you_want_to_debug)
    the_test_you_want_to_debug()
    print("-*# finished #*-")

# eof
