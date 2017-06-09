#!/usr/bin/python

"""
Utility functions.

"""


import time
import logging


def time_log(phrase, time_start=None):
    """
    A time_logger function so as to print info with time since elapsed if wanted,
    alongside with the current logging config.
    """
    if time_start:
        logging.info('%s in : %.2f seconds.' % (phrase, time.time() - time_start))
    else:
        logging.info('%s' % (phrase))
    return 1
