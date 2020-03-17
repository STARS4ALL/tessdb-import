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
from .      import DUP_SEQ_NUMBER, SINGLE, PAIR, DAYLIGHT, TSTAMP_FORMAT
from .utils import paging, previous_iterable

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------

def show_duplicated_no_dates(connection, options):
    row = {'name': options.name, 'reason': DUP_SEQ_NUMBER}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT rank, rejected, tstamp, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength
        FROM raw_readings_t 
        WHERE name == :name
        AND   rejected == :reason
        ''', row)
    return cursor

def show_duplicated_all_dates(connection, options):
    row = {
        'name': options.name, 
        'reason': DUP_SEQ_NUMBER, 
        'startd': options.start_date.to_is8601(TSTAMP_FORMAT), 
        'endd':   options.end_date.to_is8601(TSTAMP_FORMAT),
    }
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT rank, rejected, tstamp, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength
        FROM raw_readings_t 
        WHERE name == :name
        AND   rejected == :reason
        AND   tstamp BETWEEN :startd AND :endd
        ''', row)
    return cursor

def show_duplicated_since_date(connection, options):
    row = {
        'name': options.name, 
        'reason': DUP_SEQ_NUMBER, 
        'startd': options.start_date.to_is8601(TSTAMP_FORMAT), 
    }
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT rank, rejected, tstamp, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength
        FROM raw_readings_t 
        WHERE name == :name
        AND   rejected == :reason
        AND   tstamp >= :startd
        ''', row)
    return cursor

def show_duplicated_until_date(connection, options):
    row = {
        'name': options.name, 
        'reason': DUP_SEQ_NUMBER, 
        'startd': options.start_date.to_is8601(TSTAMP_FORMAT), 
        'endd':   options.end_date.to_is8601(TSTAMP_FORMAT),
    }
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT rank, rejected, tstamp, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength
        FROM raw_readings_t 
        WHERE name == :name
        AND   rejected == :reason
        AND   tstamp <= :endd
        ''', row)
    return cursor



def show_count_valid(connection, name):
    cursor = connection.cursor()
    if name is None:
        cursor.execute(
            '''
            SELECT name, COUNT(*) FROM raw_readings_t
            WHERE rejected IS NULL
            GROUP BY name
            ''')
    else:
        row = {'name': name }
        cursor.execute(
            '''
            SELECT name, COUNT(*) FROM raw_readings_t
            WHERE name == :name
            AND WHERE rejected IS NULL
            ''', row)
    paging(cursor,["Name", "Count"])


def show_count_reason(connection, name, reason):
    cursor = connection.cursor()
    if name is None:
        row = {'reason': reason }
        cursor.execute(
            '''
            SELECT name, COUNT(*) FROM raw_readings_t
            WHERE rejected == :reason
            GROUP BY name
            ''',row)
    else:
        row = {'name': name, 'reason': reason }
        cursor.execute(
            '''
            SELECT name, COUNT(*) FROM raw_readings_t
            WHERE name == :name
            AND rejected == :reason
            ''', row)
    return cursor

def show_count_duplicated(connection, name):
    iterable = show_count_reason(connection, name, DUP_SEQ_NUMBER)
    paging(iterable,["Name", "Count"])


def show_count_single(connection, name):
    iterable = show_count_reason(connection, name, SINGLE)
    paging(iterable,["Name", "Count"])


def show_count_pairs(connection, name):
    iterable = show_count_reason(connection, name, PAIR)
    paging(iterable,["Name", "Count"])


def show_count_daylight(connection, name):
    iterable = show_count_reason(connection, name, DAYLIGHT)
    paging(iterable,["Name", "Count"])


# ==============
# MAIN FUNCTIONS
# ==============

def show_global(connection, options):
    cursor = connection.cursor()
    if options.name is None:
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
    paging(cursor,["Name","Median Period (s)", "Sample Count", "Compute method"], options.limit)


def show_differences(connection, options):
    cursor = connection.cursor()
    if options.name is None:
        cursor.execute(
            '''
            SELECT name, rank, tstamp, delta_seq, delta_T, period
            FROM first_differences_t
            ORDER BY name ASC
            ''')
    else:
        row = {'name': options.name}
        cursor.execute(
            '''
            SELECT name, rank, tstamp, delta_seq, delta_T, period
            FROM first_differences_t
            WHERE name == :name
            ''', row)
    paging(cursor,["Name","Rank", "Timestamp", u"\u0394 Seq.", u"\u0394 T", "Period"], options.limit)



def show_daily(connection, options):
    cursor = connection.cursor()
    if options.name is None:
        cursor.execute(
            '''
            SELECT name, date_id, max_period, min_period, median_period, mean_period, stddev_period, N
            FROM daily_stats_t
            ORDER BY name ASC
            ''')
    else:
        row = {'name': options.name}
        cursor.execute(
            '''
            SELECT name, date_id, max_period, min_period, median_period, mean_period, stddev_period, N
            FROM daily_stats_t
            WHERE name == :name
            ''', row)
    paging(cursor,["Name","Date Id", "Max. T", "Min. T", "Median T", "Average T", "StdDev T", "N"], options.limit)


def show_duplicated(connection, options):
    if options.start_date is None and options.end_date is None:
        cursor = show_duplicated_no_dates(connection, action)
    elif options.start_date is None and options.end_date is not None:
        cursor = show_duplicated_until_date(connection, action)
    elif options.start_date is not None and options.end_date is None:
        cursor = show_duplicated_since_date(connection, action)
    else:
        cursor = show_duplicated_all_dates(connection, action)
    paging(cursor,["Rank","Rejection", "Timestamp", "Name", "#Sequence", "Freq", "Mag", "TAmb", "TSky", "RSS"], maxsize=options.limit)


def show_around(connection, options):
    row = {'name': options.name, 'rank': options.rank, 'width':  options.width}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT rank, rejected, tstamp, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength
        FROM raw_readings_t 
        WHERE name == :name
        AND   rank  BETWEEN :rank - :width AND :rank + :width
        ''', row)
    paging(cursor,["Rank","Rejection", "Timestamp", "Name", "#Sequence", "Freq", "Mag", "TAmb", "TSky", "RSS"], maxsize=2*options.width+1)


def show_count(connection, options):
    if options.valid:
        show_count_valid(connection, options.name)
    elif options.duplicated:
        show_count_duplicated(connection, options.name)
    elif options.single:
        show_count_single(connection, options.name)
    elif options.pairs:
        show_count_pairs(connection, options.name)
    elif options.daylight:
        show_count_daylight(connection, options.name)
    else:
        pass
  