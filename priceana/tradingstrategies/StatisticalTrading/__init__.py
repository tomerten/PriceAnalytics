# -*- coding: utf-8 -*-

"""
Package tradingstrategies - module StatisticalTrading
=====================================================

Module containing code for trading strategies based on 
statistical analysis.
"""

import numpy as np
import pandas as pd
import statsmodels.api as sm
from numpy import log, polyfit, sqrt, std, subtract
from priceana.utils.LoggingUtils import logger
from scipy.optimize import curve_fit
from termcolor import colored


def hurst(ts: np.array) -> float:
    """
    Returns the Hurst Exponent of the time series vector ts.

    Args:
        - ts : timeseries in numpy array format
    """
    # Create the range of lag values
    lags = range(2, 20)

    # Calculate the array of the variances of the lagged differences
    tau = [sqrt(std(subtract(ts[lag:], ts[:-lag]))) for lag in lags]
    print(tau)
    # Use a linear fit to estimate the Hurst Exponent
    poly = polyfit(log(lags), log(tau), 1)

    # Return the Hurst exponent from the polyfit output
    return poly[0] * 2.0


def strategy_mean_reverting(ohlcv, months=5, verbose=True, show=True):
    """

    :param ohlcv: prepared ohlcv data
    :param months: number of (last) months to select for using in the test
    :param verbose:
    :param show:
    :return:

    # H<0.5 The time series is mean reverting
    # H=0.5 The time series is a Geometric Brownian Motion
    # H>0.5 The time series is trending
    """
    ohlcv = ohlcv.reset_index()
    ohlcv["formatted_date"] = pd.to_datetime(ohlcv["date"])
    ohlcv = ohlcv.set_index(pd.to_datetime(ohlcv["formatted_date"].dt.date))
    ohlcv = ohlcv[~ohlcv.index.duplicated()]
    ohlcv = ohlcv.reset_index(drop=True)
    # select last three months

    ohlcv_red = ohlcv[
        ohlcv["formatted_date"]
        > (ohlcv["formatted_date"].max() - pd.offsets.DateOffset(months=months))
    ].copy()

    print(ohlcv_red)
    h = hurst(ohlcv_red["close"].values)
    logger.info("Hurst:  {}".format(h))

    if h < 0.5:

        # define linear fit function
        def f(x, A, B):  # this is your 'straight line' y=f(x)
            return A * x + B

        # create new column days_since three months for easier fitting
        ohlcv_red["days_since"] = (
            ohlcv_red["formatted_date"]
            - (ohlcv_red["formatted_date"].max() - pd.offsets.DateOffset(months=months))
        ).astype("timedelta64[D]")

        # use scipy to fit
        A, B = curve_fit(f, ohlcv_red["days_since"], ohlcv_red["close"])  # your data x, y to fit

        # print scipy fitting if verbose
        if verbose:
            logger.info("a = {} +/- {}".format(A[0], B[0, 0] ** 0.5))
            logger.info("b = {} +/- {}".format(A[1], B[1, 1] ** 0.5))
            logger.info("b = ", A[1], "+/-", B[1, 1] ** 0.5)

        # use least square from statsmodel
        sm.OLS(ohlcv_red["days_since"], ohlcv_red["close"])

        # calculate prices
        stoplossprice = (A[0] - 2 * B[0, 0] ** 0.5) * ohlcv_red["days_since"].max() + (
            A[1] - 2 * B[1, 1] ** 0.5
        )
        buylimitorderprice = (A[0] - B[0, 0] ** 0.5) * ohlcv_red["days_since"].max() + (
            A[1] - B[1, 1] ** 0.5
        )
        sellpricehalf1 = (A[0]) * ohlcv_red["days_since"].max() + (A[1])
        sellpricehalf2 = (A[0] + B[0, 0] ** 0.5) * ohlcv_red["days_since"].max() + (
            A[1] + B[1, 1] ** 0.5
        )

        return (
            True,
            h,
            A,
            B,
            ohlcv_red,
            stoplossprice,
            buylimitorderprice,
            sellpricehalf1,
            sellpricehalf2,
        )

    else:
        logger.info("Hurst:  {}".format(h))
        logger.info("Not mean reverting")
        if h > 0.5:
            if h > 0.75:
                logger.info(colored("Strong trending", "green"))
            else:
                logger.info(colored("Trending", "blue"))
        return False


def mr_indicator(ohlcv_red, A, B):
    def f(x, A, B):  # this is your 'straight line' y=f(x)
        return A * x + B

    ohlcv_red["pos_sig_1"] = f(ohlcv_red["days_since"], A[0], A[1])
    ohlcv_red["neg_sig_1"] = f(
        ohlcv_red["days_since"], A[0] - B[0, 0] ** 0.5, A[1] - B[1, 1] ** 0.5
    )
    ohlcv_red["pos_sig_2"] = f(
        ohlcv_red["days_since"], A[0] + B[0, 0] ** 0.5, A[1] + B[1, 1] ** 0.5
    )
    ohlcv_red["neg_sig_2"] = f(
        ohlcv_red["days_since"], A[0] - 2 * B[0, 0] ** 0.5, A[1] - 2 * B[1, 1] ** 0.5
    )

    diff_neg_sig_1 = (
        ohlcv_red["close"] < ohlcv_red["neg_sig_1"]
    )  # & (ohlcv_red['close'] > ohlcv_red['neg_sig_2'])
    diff_neg_sig_2 = ohlcv_red["close"] < ohlcv_red["neg_sig_2"]
    diff_pos_sig_1 = ohlcv_red["close"] > ohlcv_red["pos_sig_1"]
    diff_pos_sig_2 = ohlcv_red["close"] > ohlcv_red["pos_sig_2"]

    diff_neg_sig_1_forward = diff_neg_sig_1.shift(1)
    diff_neg_sig_2_forward = diff_neg_sig_2.shift(1)
    diff_pos_sig_1_forward = diff_pos_sig_1.shift(1)
    diff_pos_sig_2_forward = diff_pos_sig_2.shift(1)

    ohlcv_red["diff_neg_sig_1"] = diff_neg_sig_1 - diff_neg_sig_1_forward
    ohlcv_red["diff_neg_sig_2"] = diff_neg_sig_2 - diff_neg_sig_2_forward
    ohlcv_red["diff_pos_sig_1"] = diff_pos_sig_1 - diff_pos_sig_1_forward
    ohlcv_red["diff_pos_sig_2"] = diff_pos_sig_2 - diff_pos_sig_2_forward

    conditions = [
        ohlcv_red["diff_neg_sig_2"] == 1,
        (ohlcv_red["diff_neg_sig_1"] == -1),
        ohlcv_red["diff_pos_sig_1"] == -1,
        ohlcv_red["diff_pos_sig_2"] == -1,
    ]
    choices = [-1, 1, -0.5, -0.5]

    indlist = np.select(conditions, choices, default=0)

    total = 0
    ind = []
    for v in indlist:
        if (total + v) < 0.0:
            ind.append(0)
        elif (total + v) > 1.0:
            ind.append(0)
        elif (total == 0.5) and (v == -1):
            total += -0.5
            ind.append(-0.5)
        else:
            total += v
            ind.append(v)

    ohlcv_red["mr_ind"] = ind

    return ohlcv_red
