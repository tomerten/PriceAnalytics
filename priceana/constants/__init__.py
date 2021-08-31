# -*- coding: utf-8 -*-

"""
Module priceana.constants 
=================================================================

A module containing the constants used in this package, this
mostly refers to the base URLs used to download the data. 

"""
import numpy as np

base_url = "https://query2.finance.yahoo.com/v8/finance/"
query_url = "https://query2.finance.yahoo.com/v10/finance/quoteSummary/"

yearly_keys = [
    "assetProfile",
    "balanceSheetHistory",
    "cashflowStatementHistory",
    "incomeStatementHistory",
    "indexTrend",
    "industryTrend",
    "quoteType",
    "sectorTrend",
]

quarterly_keys = [
    "balanceSheetHistoryQuarterly",
    "cashflowStatementHistoryQuarterly",
    "incomeStatementHistoryQuarterly",
    "calendarEvents",
    "earnings",
    "earningsHistory",
    "netSharePurchaseActivity",
    "secFilings",
]

monthly_keys = [
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
]

weekly_keys = ["earningsTrend"]

daily_keys = ["financialData", "price", "summaryDetail"]

all_keys = sorted(daily_keys + weekly_keys + monthly_keys + quarterly_keys + yearly_keys)

# ORDER IS IMPORTANT AS IT IS USED TO CHECK IF DATA IS AVAILABLE FOR THIS PERIOD
valid_periods = ["1d", "5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "10y", "ytd", "max"]

valid_intervals = [
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
]

financial_period_keys = {
    "daily": daily_keys,
    "weekly": weekly_keys,
    "monthly": monthly_keys,
    "quarterly": quarterly_keys,
    "yearly": yearly_keys,
    "all": all_keys,
}

# ------------------- CONSTANTS ----------------------------------
MS_SUBSTITUTIONS = {
    "%": "",
    "Mil": "",
    "*": "",
    "(": "",
    ")": "",
    "{": "",
    "}": "",
    "'": "",
    '"': "",
    "&": "",
    "-": "",
    " ": "",
    "\xa0": "",
}

CURRENCIES = np.array(
    [
        "AED",
        "ARS",
        "AUD",
        "BDT",
        "BGN",
        "BRL",
        "CAD",
        "CHF",
        "CNY",
        "COP",
        "CZK",
        "DKK",
        "EGP",
        "EUR",
        "GBP",
        "GEL",
        "GHS",
        "HKD",
        "HUF",
        "IDR",
        "ILS",
        "INR",
        "JMD",
        "JPY",
        "KRW",
        "KWD",
        "KZT",
        "MAD",
        "MXN",
        "MYR",
        "NGN",
        "NOK",
        "NZD",
        "OMR",
        "PEN",
        "PHP",
        "PKR",
        "PLN",
        "RON",
        "RUB",
        "SAR",
        "SEK",
        "SGD",
        "THB",
        "TRY",
        "TWD",
        "UAH",
        "USD",
        "VND",
        "ZAR",
    ]
)

CURRENCIES_DC = {k: "" for k in CURRENCIES}
