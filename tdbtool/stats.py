# -*- coding: utf-8 -*-

# TESS UTILITY TO PERFORM SOME MAINTENANCE COMMANDS

# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

from __future__ import generators    # needs to be at the top of your module


import os
import os.path
import sys
import argparse
import sqlite3
import logging
import csv
import traceback

# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__
from .utils import paging

# ----------------
# Module constants
# ----------------

TSTAMP_FORMAT  = "%Y-%m-%dT%H:%M:%SZ"
DUP_SEQ_NUMBER = "Dup Sequence Number"
SINGLE = "Single"
PAIR   = "Pair"

# -----------------------
# Module global variables
# -----------------------


def automatic_global_stats(connection):
    row = {'method': "Automatic"}
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR REPLACE INTO global_stats_t(name, median_period, method, N)
        SELECT name, MEDIAN(median_period), :method, COUNT(*)
        FROM  daily_stats_t
        GROUP BY name
        ''', row)
    connection.commit()


def manual_global_stats(connection, name, period):
    row = {'name': name, 'period': period, 'method': "Manual", 'N':0}
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR REPLACE INTO global_stats_t(name, median_period, method, N)
        VALUES (:name, :period, :method, :N)
        ''',row)
    connection.commit()


def display_global_stats(connection, name):
    cursor = connection.cursor()
    if name is None:
        cursor.execute(
            '''
            SELECT name, median_period, N, method
            FROM global_stats_t
            ORDER BY name ASC
            ''')
    else:
        row = {'name': name}
        cursor.execute(
            '''
            SELECT name, median_period, N, method
            FROM global_stats_t
            WHERE name == :name
            ''')
    paging(cursor,["Name","Median Period (s)", "Sample Count", "Compute method"])


# ==============
# MAIN FUNCTIONS
# ==============

def stats_daily(connection, options):
    logging.info("[{0}] computing daily period statistics".format(__name__))
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR REPLACE INTO daily_stats_t(name, date_id, mean_period, median_period, stddev_period, N, quality)
        SELECT name, date_id, AVG(seconds_diff), MEDIAN(seconds_diff), STDEV(seconds_diff), COUNT(*), MEDIAN(seconds_diff)/STDEV(seconds_diff)
        FROM  first_differences_t
        GROUP BY name, date_id
        ''')
    connection.commit()
    logging.info("[{0}] Done!".format(__name__))


def stats_global(connection, options):
    logging.info("[{0}] computing global period statistics".format(__name__))
    if options.name is not None:
        if options.period is not None:
            manual_global_stats(connection, options.name, options.period)
        else:
            logging.error("[{0}] a period must be specified with --period".format(__name__))
    else:
        automatic_global_stats(connection)
    display_global_stats(connection, options.name)
    logging.info("[{0}] Done!".format(__name__))

def stats_retained(connection, options):
    pass