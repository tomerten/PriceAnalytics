#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `priceana` package."""

import priceana.constants
import pytest


def test_base_url():
    assert priceana.constants.base_url == "https://query2.finance.yahoo.com/v8/finance/"


def test_query_url():
    assert (
        priceana.constants.query_url
        == "https://query2.finance.yahoo.com/v10/finance/quoteSummary/"
    )


testdata = [
    (
        priceana.constants.yearly_keys,
        [
            "assetProfile",
            "balanceSheetHistory",
            "cashflowStatementHistory",
            "incomeStatementHistory",
            "indexTrend",
            "industryTrend",
            "quoteType",
            "sectorTrend",
        ],
    ),
    (
        priceana.constants.quarterly_keys,
        [
            "balanceSheetHistoryQuarterly",
            "cashflowStatementHistoryQuarterly",
            "incomeStatementHistoryQuarterly",
            "calendarEvents",
            "earnings",
            "earningsHistory",
            "netSharePurchaseActivity",
            "secFilings",
        ],
    ),
    (
        priceana.constants.monthly_keys,
        [
            "defaultKeyStatistics",
            "esgScores",
            "fundOwnership",
            "insiderHolders",
            "insiderTransactions",
            "institutionOwnership",
            "majorDirectHolders",
            "majorHoldersBreakdown",
            "recommendationTrend",
            "upgradeDowngradeHistory",
        ],
    ),
    (priceana.constants.weekly_keys, ["earningsTrend"]),
    (priceana.constants.daily_keys, ["financialData", "price", "summaryDetail"]),
    (
        priceana.constants.valid_periods,
        ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"],
    ),
    (
        priceana.constants.valid_intervals,
        [
            "1m",
            "2m",
            "5m",
            "15m",
            "30m",
            "90m",
            "1h",
            "1d",
            "5d",
            "1wk",
            "1mo",
            "3mo",
            "all",
        ],
    ),
]


@pytest.mark.parametrize("v,e", testdata)
def test_yearly_keys(v, e):
    assert v == e


# ==============================================================================
# The code below is for debugging a particular test in eclipse/pydev.
# (normally all tests are run with pytest)
# ==============================================================================
if __name__ == "__main__":
    the_test_you_want_to_debug = test_yearly_keys

    the_test_you_want_to_debug()
    print("-*# finished #*-")
# ==============================================================================
