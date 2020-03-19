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
from .      import DAYLIGHT
from .utils import paging, shift_generator

# ----------------
# Module constants
# ----------------

SHIFT_SIZE      = 7
ROWS_PER_COMMIT = 50000

# -----------------------
# Module global variables
# -----------------------


def is_monotonic(aList):
    # Calculate first difference
    # Modified second difference with absolute values, to avoid cancellation 
    # in final sum due to symmetric differences
    first_diff  = tuple(aList[i+1] - aList[i] for i in xrange(len(aList)-1))
    second_diff = tuple(abs(first_diff[i+1] - first_diff[i])  for i in xrange(len(first_diff)-1))
    return sum(second_diff) == 0


def is_invalid(iterable):
    '''
    Invalid magnitudes have a value of zero
        '''
    return sum(iterable) == 0


def candidate_names_iterable(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name
        FROM raw_readings_t 
        ORDER BY name ASC
        ''')
    return cursor


def candidates_iterable(connection, name):
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT name, date_id, time_id, sequence_number, frequency, magnitude
        FROM  raw_readings_t 
        WHERE name == :name
        AND   rejected IS NULL
        ORDER BY date_id ASC, time_id ASC
        ''', row)
    return cursor


def mark_daylight(connection, iterable):
    logging.debug("[{0}] Marking daylight for {1} rows".format(__name__, len(iterable)))
    cursor = connection.cursor()
    cursor.executemany(
        '''
        UPDATE raw_readings_t
        SET rejected = :reason
        WHERE name == :name
        AND   date_id == :date_id
        AND   time_id == :time_id
         ''', iterable)
    connection.commit()


def daylight_detect_by_name(connection, name):
    logging.info("[{0}] detecting daylight readings for {1}".format(__name__, name))
    count = 0
    rows = []
    iterable1 = candidates_iterable(connection, name)
    for points in shift_generator(iterable1, SHIFT_SIZE):
        if not all(points):
            continue
        magnitudes  = tuple(point[5] for point in points)
        seq_numbers = tuple(point[3] for point in points)
        if is_monotonic(seq_numbers) and is_invalid(magnitudes):
            if len(rows)  < ROWS_PER_COMMIT:
                chosen = points[SHIFT_SIZE//2]
                row    = {'name': chosen[0], 'date_id': chosen[1], 'time_id': chosen[2], 'reason': DAYLIGHT}
                rows.append(row)
            else:
                count += ROWS_PER_COMMIT
                mark_daylight(connection, rows)
                rows = []
    if len(rows):
        count += len(rows)
        mark_daylight(connection, rows)
    logging.info("[{0}] Detected {1} daylight readings for {2}. Done!".format(__name__, count, name))


# ==============
# MAIN FUNCTIONS
# ==============

def daylight_detect(connection, options):
    if options.name is not None:
        daylight_detect_by_name(connection, options.name)
    else:
        for name in candidate_names_iterable(connection):
            daylight_detect_by_name(connection, name[0])


