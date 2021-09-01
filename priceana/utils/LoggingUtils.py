# -*- coding: utf-8 -*-

"""
Module priceana.utils.LoggingUtils
=================================================================

A module containing methods for logging.   

"""
import logging
import sys

logging.basicConfig(
    format="%(asctime)s %(levelname)s:%(name)s: %(message)s",
    # level=logging.INFO,  # logging.DEBUG,
    level=logging.DEBUG,
    datefmt="%H:%M:%S",
    stream=sys.stderr,
)

logger = logging.getLogger("Pipeline")
logging.getLogger("chardet.charsetprober").disabled = True
