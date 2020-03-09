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

# Access  template withing the package
from pkg_resources import resource_filename

# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__
from .utils import paging, tuple_generator

# ----------------
# Module constants
# ----------------

TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# -----------------------
# Module global variables
# -----------------------



# --------------
# Module classes
# --------------
class datetime(tdbtool.s4a.datetime):


    def to_seconds(self):
        '''
        Return date and time database identifiers + seconds within the day
        '''
        return 3600*self.hour + 60*self.minute + self.second



class Counter(object):
    '''A counter to inject in database'''

    def __init__(self, value, max_date_id):
        self._value = value
        self._max_date_id = max_date_id

    def next(self):
        self._value += 1
        return self._value

    def prev(self):
        self._value -= 1
        return self._value

    def current(self):
        return self._value

    def update_date_id(self, date_id):
        self._max_date_id = date_id if date_id > self._max_date_id else self._max_date_id

    def date_id(self):
        return self._max_date_id



class CounterFactory(object):
    '''Per photometer counter factory'''

    def __init__(self, connection):
        self._pool = {}
        self._connection = connection


    def loadMax(self, name):
        cursor = self._connection.cursor()
        row = {'name': name}
        try:
            cursor.execute(
                '''
                SELECT max_id, max_date_id FROM housekeeping_t
                WHERE tess == :name
                ''', row)
        except Exception as e:
            logging.info("[{0}] table does not exist".format(__name__))
            row['max_id']      = 0
            row['max_date_id'] = 0
        else:
            result = cursor.fetchone()
            if result is not None:
                row['max_id']      = result[0]
                row['max_date_id'] = result[1]
            else:
                row['max_id']      = 0
                row['max_date_id'] = 0
            logging.info("[{0}] Loading Counters {1}".format(__name__, row))
        return row['max_id'], row['max_date_id']


    def saveMax(self):
        cursor = self._connection.cursor()
        for key in self._pool.keys():
            try:
                row = {
                    'name': key, 
                    'max_id': self._pool[key].current()-1,
                    'max_date_id' : self._pool[key].date_id()   
                }
                cursor.execute(
                    '''
                    INSERT OR REPLACE INTO housekeeping_t(tess, max_id, max_date_id) 
                    VALUES (:name, :max_id, :max_date_id)
                    ''', row)
                logging.info("[{0}] Saving counters {1}".format(__name__, row))
            except Exception as e:
                logging.error("[{0}] Error saving counters".format(__name__))
                traceback.print_exc()
        


    def build(self, name):
        if name not in self._pool.keys():
            value, max_date_id = self.loadMax(name)
            c = Counter(value+1, max_date_id)
            self._pool[name] = c
        return self._pool[name]

# -----------------------
# Module global functions
# -----------------------

def csv_generator(filepath, factory):
    '''An iterator that reads csv line by line and keeps memory usage down'''
    with open(filepath, "r") as csvfile:
        datareader = csv.reader(csvfile,delimiter=';')
        dummy = next(datareader)  # drops the header row
        for srcrow in datareader:
            row = []
            dt = datetime.from_iso8601(srcrow[0], TSTAMP_FORMAT)
            #dateid, timeid, seconds = datetime.from_iso8601(srcrow[0],TSTAMP_FORMAT).to_dbase_ids()
            dateid, timeid = dt.to_dbase_ids()
            seconds = dt.to_seconds()
            counter = factory.build(srcrow[1])
            row.append(counter.current())
            row.append(dateid)
            row.append(timeid)
            row.append(srcrow[1])         # name = 3
            row.append(int(srcrow[2]))    # sequence number = 4
            row.append(float(srcrow[3]))  # frequency = 5
            row.append(float(srcrow[4]))  # magnitude = 6
            row.append(float(srcrow[5]))  # tamb = 7
            row.append(float(srcrow[6]))  # tsky = 8
            row.append(seconds)           # number of seconds within the day = 9
            try:
                val = int(srcrow[7])
            except Exception as e:
                val = None
            row.append(val)                # RSS  = 10
            yield row


def name_and_date_iterable(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT  tess, date_id, count(*)
        FROM    raw_readings_t
        GROUP BY tess, date_id
        ORDER BY tess ASC, date_id ASC
        ''')
    return cursor   # return Cursor as an iterable


def daily_iterable(connection, name, date_id):
    row = {'name': name, 'date_id': date_id}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT tess, date_id, seconds, sequence_number, id
        FROM  raw_readings_t
        WHERE tess = :name
        AND   date_id = :date_id
        ''', row)
    return cursor   # return Cursor as an iterable



def write_daily_differences(connection, prev, cur, N):
    row = {
        'name'    : cur[0],
        'date_id' : cur[1],
        'time_id' : cur[2],
        'id'      : cur[4],
        'deltaSeq': cur[3] - prev[3],
        'deltaT'  : cur[2] - prev[2],
        'period'  : float(cur[2] - prev[2])/(cur[3] - prev[3]),
        'N'       : N,
    }
    #logging.info("[{0}] {4}-1 Differences for {1}, {2}-{3} ".format(__name__, row['name'], row['date_id'], row['time_id'], row['N']))
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR IGNORE INTO first_differences_t(tess, date_id, time_id, id, seq_diff, seconds_diff, period, N)
         VALUES(
            :name,
            :date_id,
            :time_id,
            :id,
            :deltaSeq,
            :deltaT,
            :period,
            :N
        )
        ''', row)
    connection.commit()


def compute_stats(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR IGNORE INTO stats_t(tess, date_id, mean_period, median_period, stddev_period)
        SELECT tess, date_id, SUM(seconds_diff)/COUNT(*), MEDIAN(seconds_diff), STDEV(seconds_diff)
        FROM  first_differences_t
        GROUP BY tess, date_id
        ''')
    connection.commit()   # return Cursor as an iterable

# ==============
# MAIN FUNCTIONS
# ==============


def input_slurp(connection, options):
    logging.info("[{0}] Starting ingestion from {1}".format(__name__, options.csv_file))
    duplicates = {}
    max_date_id = {}
    cursor = connection.cursor()
    factory = CounterFactory(connection)
    for row in csv_generator(options.csv_file, factory):
        try:
            counter = factory.build(row[3])
            max_date_id = counter.date_id()
            if row[1] < max_date_id:
                # Skip old data
                continue
            else:
                counter.next()
                cursor.execute(
                    '''
                    INSERT INTO raw_readings_t(id, date_id, time_id, tess, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, seconds, signal_strength) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?)
                    ''', row)
        except sqlite3.IntegrityError as e:
            duplicates[row[3]] = duplicates.get(row[3],0) + 1
            oldv = counter.prev()
            logging.debug("[{0}] Duplicated row, restoring counter for {1} to {2}".format(__name__, row[3], oldv))
        else:
            counter.update_date_id(row[1])
    logging.info("[{0}] Ended ingestion from {1}".format(__name__, options.csv_file))
    logging.info("[{0}] Saving house keeping data".format(__name__))
    factory.saveMax()
    connection.commit()
    logging.info("[{0}] Duplicates summary: {1}".format(__name__, duplicates))
    #paging(cursor,["TESS","MAC","Site"])


def input_stats(connection, options):
    cursor = connection.cursor()
    logging.info("[{0}] Starting Tx period stats calculation".format(__name__))
    for group in name_and_date_iterable(connection):
        name    = group[0]
        date_id = group[1]
        N       = group[2]
        LL      = N / 20
        i       = 0
        logging.info("[{0}] Computing diff for {1} and {2} ({3} points)".format(__name__, name, date_id, N))
        for point in tuple_generator(daily_iterable(connection, name, date_id), 2):
            if not all(point):
                continue
            else:
                prev, cur = point
                i = (i + 1) % LL
                if i == 0:
                    logging.info("[{0}] {1}: date = {2}, id = {3}".format(__name__, cur[0], cur[1], cur[4]))
                write_daily_differences(connection, prev, cur, N)
    compute_stats(connection)
    logging.info("[{0}] Done!".format(__name__))

                


   

   




