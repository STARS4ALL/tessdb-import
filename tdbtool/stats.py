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
from .      import DUP_SEQ_NUMBER, SINGLE, PAIR, TSTAMP_FORMAT
from .utils import paging, previous_iterable, candidate_names_iterable

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

def stats_global_auto(connection, name):
    cursor = connection.cursor()
    if name is None:
        logging.info("[{0}] Computing global period statistics for all photometers".format(__name__))
        row = {'method': "Automatic"}
        cursor.execute(
            '''
            INSERT OR REPLACE INTO global_stats_t(name, median_period, method, N)
            SELECT name, MEDIAN(median_period), :method, COUNT(*)
            FROM  daily_stats_t
            GROUP BY name
            ''', row)
    else:
        logging.info("[{0}] Computing global period statistics for {1} photometer".format(__name__, name))
        row = {'name': name, 'method': "Automatic"}
        cursor.execute(
            '''
            INSERT OR REPLACE INTO global_stats_t(name, median_period, method, N)
            SELECT name, MEDIAN(median_period), :method, COUNT(*)
            FROM  daily_stats_t
            WHERE name == :name
            GROUP BY name
            ''', row)
    connection.commit()
    logging.info("[{0}] Done!".format(__name__))


def stats_global_manual(connection, name, period):
    logging.info("[{0}] Setting global period statistics to {1} for {2} photometer".format(__name__, period, name))
    row = {'name': name, 'period': period, 'method': "Manual", 'N':0}
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR REPLACE INTO global_stats_t(name, median_period, method, N)
        VALUES (:name, :period, :method, :N)
        ''',row)
    connection.commit()
    logging.info("[{0}] Done!".format(__name__))

    
# ==============
# MAIN FUNCTIONS
# ==============

def stats_daily(connection, options):
    cursor = connection.cursor()
    if options.name is None:
        logging.info("[{0}] Computing daily period statistics".format(__name__))
        cursor.execute(
            '''
            INSERT OR REPLACE INTO daily_stats_t(name, date_id, mean_period, median_period, stddev_period, N, min_period, max_period)
            SELECT name, date_id, AVG(delta_T), MEDIAN(delta_T), STDEV(delta_T), COUNT(*), MIN(delta_T), MAX(delta_T)
            FROM  first_differences_t
            GROUP BY name, date_id
            ''')
    else:
        logging.info("[{0}] Computing daily period statistics for {1}".format(__name__, options.name))
        row = {'name': options.name }
        cursor.execute(
            '''
            INSERT OR REPLACE INTO daily_stats_t(name, date_id, mean_period, median_period, stddev_period, N, min_period, max_period)
            SELECT name, date_id, AVG(delta_T), MEDIAN(delta_T), STDEV(delta_T), COUNT(*), MIN(delta_T), MAX(delta_T)
            FROM  first_differences_t
            WHERE name == :name
            GROUP BY name, date_id
            ''', row)
    connection.commit()
    logging.info("[{0}] Done!".format(__name__))



def stats_global(connection, options):
    stats_global_auto(connection, options.name)
