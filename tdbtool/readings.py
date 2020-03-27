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
import sqlite3
import logging
import collections

# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__
from .      import COINCIDENT, SHIFTED, AMBIGUOUS_TIME
from .utils import open_database, open_reference_database, mark_bad_rows
from .utils import candidate_names_iterable, PeriodDAO

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 10000

# -----------------------
# Module global variables
# -----------------------


# --------------
# Module classes
# --------------




# -----------------------
# Module global functions
# -----------------------


def good_readings_iterable(connection, name):
    '''Used to find out location_id values'''
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT date_id, time_id, tess_id, sequence_number
        FROM  raw_readings_t
        WHERE rejected IS NULL
        AND   accepted IS NULL
        AND name == :name
        ORDER BY date_id ASC, time_id ASC
        ''', row)
    return cursor


def find_sequence_number(connection, tess_id, tstamp, period, seq_num):
    row = {'tess_id': tess_id, 'tstamp': tstamp, 'seq_num': seq_num}
    row['high'] = str(period/2)  + ' seconds'
    row['low']  = str(-period/2) + ' seconds'
    cursor = connection.cursor()
    cursor.execute('''
        SELECT sequence_number, datetime(iso8601fromids(date_id, time_id)) as tstamp
        FROM tess_readings_t
        WHERE tess_id == :tess_id
        AND tstamp BETWEEN datetime(:tstamp, :low) AND datetime(:tstamp, :high)
        ORDER BY tstamp
        ''', row)
    return cursor.fetchall()


def mark_ok_rows(connection, ok_rows):
    name = ok_rows[0]['name']
    cursor = connection.cursor()
    cursor.executemany('''
        UPDATE raw_readings_t
        SET accepted = :flag
        WHERE name  == :name
        AND date_id == :date_id
        AND time_id == :time_id
        ''', ok_rows)
    connection.commit()


def readings_compare_by_name(connection, name, connection2):
    dup_sequence_ids = []
    ok_sequence_ids  = []
    bad_count    = 0
    good_count   = 0
    periodDAO = PeriodDAO(connection)
    logging.info("[{0}] Comparing readings in reference database for {1}".format(__name__, name))
    for date_id, time_id, tess_id, seq_num in good_readings_iterable(connection, name):
        period = periodDAO.getPeriod(name, date_id)
        tstamp = tdbtool.s4a.datetime.from_dbase_ids(date_id, time_id).to_iso8601()
        result = find_sequence_number(connection2, tess_id, tstamp, period, seq_num)
        if  not result:
            good_row = {'name': name, 'date_id': date_id, 'time_id': time_id, 'flag': 1}
            ok_sequence_ids.append(good_row)
            if len(ok_sequence_ids) == ROWS_PER_COMMIT:
                logging.info("[{0}] Marking OK  readings for {1} until {2}".format(__name__, name, date_id))
                good_count += ROWS_PER_COMMIT
                mark_ok_rows(connection, ok_sequence_ids)
                ok_sequence_ids = []
            continue
        if len(result) > 1:
            logging.warn("[{0}] Search returned {1} readings.".format(__name__, name, len(result)))
            reason = TIMESTAMP_SHIFTED
        elif result[0][0] != seq_num:
            #logging.debug("[{0}] Sequence numbers mismatch in {1}. {2} (Ref) = {3}, {4} (CSV) = {5}.".format(__name__, name, result[0][1], result[0][0], tstamp, seq_num))
            reason = SHIFTED
        else:
            reason = COINCIDENT
        bad_row = {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': reason}
        dup_sequence_ids.append(bad_row)
        if len(dup_sequence_ids) == ROWS_PER_COMMIT:
            logging.info("[{0}] Marking DUP readings for {1} until {2}".format(__name__, name, date_id))
            logging.debug("[{0}] PeriodDAO stats for {1} => {2}".format(__name__, name, periodDAO))
            bad_count += ROWS_PER_COMMIT
            mark_bad_rows(connection, dup_sequence_ids)
            dup_sequence_ids = []
    if len(ok_sequence_ids):
        good_count += len(ok_sequence_ids)
        mark_ok_rows(connection, ok_sequence_ids)
    if len(dup_sequence_ids):
        bad_count += len(dup_sequence_ids)
        mark_bad_rows(connection, dup_sequence_ids)
    logging.debug("[{0}] PeriodDAO stats for {1} => {2}".format(__name__, name, periodDAO))
    logging.info("[{0}] Accepted  {1} readings for {2}.".format(__name__, good_count, name))
    logging.info("[{0}] Discarded {1} readings for {2}.".format(__name__, bad_count, name))
    


# ==============
# MAIN FUNCTIONS
# ==============

def readings_compare(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    if options.name is not None:
        readings_compare_by_name(connection, options.name, connection2)
    else:
        for name in candidate_names_iterable(connection):
            readings_compare_by_name(connection, name[0], connection2)

