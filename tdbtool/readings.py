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
from .      import COINCIDENT
from .utils import open_database, open_reference_database, mark_bad_rows
from .utils import candidate_names_iterable

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 2000

# -----------------------
# Module global variables
# -----------------------


# --------------
# Module classes
# --------------

class PeriodDAO(object):

    def __init__(self, connection):
        self.connection  = connection
        

    def getPeriod(self, name, date_id):
        period = self.get_daily_period(name, date_id)
        if period is None:
            period = self.get_global_period(name)
        return period[0]


    def get_daily_period(self, name, date_id):
        row = {'name': name, 'date_id': date_id}
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT median_period
            FROM daily_stats_t
            WHERE name == :name
            AND date_id == :date_id
            ''', row)
        return cursor.fetchone()


    def get_global_period(self, name):
        row = {'name': name}
        cursor = self.connection.cursor()
        cursor.execute('''
            SELECT median_period
            FROM global_stats_t
            WHERE name == :name
            ''', row)
        return cursor.fetchone()

    def __repr__(self):
        return "H: 0%, M: 100%"



class PeriodCachedDAO(PeriodDAO):

    def __init__(self, connection):
        super(PeriodCachedDAO, self).__init__(connection)
        self.cache = {}
        self.hits = {}
        self.miss = {}
        self.dailyHits = {}
        self.dailyMiss = {}


    def getPeriod(self, name, date_id):
        key = name + str(date_id)
        if key in self.cache:
            self.hits[key] = self.hits.get(key, 0) + 1
            return self.cache[key]
        self.miss[key] = self.miss.get(key, 0) + 1
        period = self.get_daily_period(name, date_id)
        if period is None:
            self.dailyMiss[key] = self.dailyMiss.get(key, 0) + 1
            period = self.get_global_period(name)
        else:
            self.dailyHits[key] = self.dailyHits.get(key, 0) + 1
        self.cache[key] = period[0]
        return period[0]


    def __repr__(self):
        hits = sum(self.hits.values())
        miss = sum(self.hits.values())
        dhits = sum(self.dailyHits.values())
        dmiss = sum(self.dailyMiss.values())
        H = 100.0 * hits / float(hits+miss)
        M = 100 - H
        DH = 100.0 * dhits / float(dhits+dmiss)
        DM = 100 - DH
        return "H: {0}%, M: {1}%, DH: {2}%, DM: {3}%".format(H, M, DH, DM)



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
        SELECT sequence_number
        FROM tess_readings_t
        WHERE tess_id == :tess_id
        AND datetime(iso8601fromids(date_id, time_id)) 
        BETWEEN datetime(:tstamp, :low) 
        AND datetime(:tstamp, :high)
        ''', row)
    return cursor.fetchone()


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
    logging.info("[{0}] Exploring existing readings in reference database for {1}".format(__name__, name))
    for date_id, time_id, tess_id, seq_num in good_readings_iterable(connection, name):
        period = periodDAO.getPeriod(name, date_id)
        tstamp = tdbtool.s4a.datetime.from_dbase_ids(date_id, time_id).to_iso8601()
        result = find_sequence_number(connection2, tess_id, tstamp, period, seq_num)
        if result is None:
            good_row = {'name': name, 'date_id': date_id, 'time_id': time_id, 'flag': 1}
            ok_sequence_ids.append(good_row)
            if len(ok_sequence_ids) == ROWS_PER_COMMIT:
                logging.info("[{0}] Marking OK  readings for {1} until {2}".format(__name__, name, date_id))
                good_count += ROWS_PER_COMMIT
                mark_ok_rows(connection, ok_sequence_ids)
                ok_sequence_ids = []
            continue
        if result[0] != seq_num:
            logging.info("[{0}] Something werid with sequence numbers in {1}. Ref = {2}, CSV = {3}.".format(__name__, name, result[0], seq_num))
        bad_row = {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': COINCIDENT}
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

