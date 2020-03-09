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
from .utils import paging

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

    def __init__(self, value):
        self._value = value

    def next(self):
        self._value += 1
        return self._value

    def prev(self):
        self._value -= 1
        return self._value

    def current(self):
        return self._value


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
                SELECT max_id FROM housekeeping_t
                WHERE tess == :name
                ''', row)
        except Exception as e:
            logging.info("[{0}] table does not exist".format(__name__))
            print(e)
            result = -1
        else:
            result = cursor.fetchone()
        return result[0] if result is not None else -1


    def saveMax(self, max_date_id):
        cursor = self._connection.cursor()
        for key in self._pool.keys():
            try:
                row = {'name': key, 'max_id': self._pool[key].current()-1, 'max_date_id': max_date_id[key]}
                logging.info(row)
                cursor.execute(
                    '''
                    INSERT OR REPLACE INTO housekeeping_t(tess, max_id, max_date_id) 
                    VALUES (:name, :max_id, :max_date_id)
                    ''', row)
            except Exception as e:
                logging.error("[{0}] Error saving counters".format(__name__))
                traceback.print_exc()
        


    def build(self, name):
        if name not in self._pool.keys():
            value = self.loadMax(name)
            c = Counter(value+1)
            self._pool[name] = c
        return self._pool[name]

# -----------------------
# Module global functions
# -----------------------


def pairs_generator(iterable):
    prev = None
    for cur in iterable:
        yield prev, cur
        prev = cur


def csv_generator(filepath, factory):
    '''An iterator that reads csv line by line and keeps memory usage down'''
    with open(filepath, "r") as csvfile:
        datareader = csv.reader(csvfile,delimiter=';')
        dummy = next(datareader)  # drops the header row
        for srcrow in datareader:
            row = []
            kk = datetime.from_iso8601(srcrow[0], TSTAMP_FORMAT)
            #dateid, timeid, seconds = datetime.from_iso8601(srcrow[0],TSTAMP_FORMAT).to_dbase_ids()
            dateid, timeid = kk.to_dbase_ids()
            seconds = kk.to_seconds()
            counter = factory.build(srcrow[1])
            row.append(counter.current())
            row.append(dateid)
            row.append(timeid)
            row.append(srcrow[1])         # name
            row.append(int(srcrow[2]))    # sequence number
            row.append(float(srcrow[3]))  # frequency
            row.append(float(srcrow[4]))  # magnitude
            row.append(float(srcrow[5]))  # tamb
            row.append(float(srcrow[6]))  # tsky
            row.append(seconds)           # number of seconds within the day
            try:
                val = int(srcrow[7])
            except Exception as e:
                val = None
            row.append(val)    # RSS
            counter.next()  
            yield row


def by_name_and_day(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT  tess, date_id, count(*)
        FROM    raw_readings_t
        GROUP BY tess, date_id
        ORDER BY tess ASC, date_id ASC
        ''')
    return cursor   # return Cursor as an iterator

def kk(connection, iterable):
    for item in iterable:
        row = {'name': item[0], 'date_id': item[1]}
        logging.info("[{0}] Calculating first differences for {1} on {2}".format(__name__, item[0], item[1]))
        cursor = connection.cursor()
        cursor.execute(
            '''
            INSERT INTO first_differences_t()
            SELECT dst.tess, dst.date_id, dst.time_id, dst.id, (dst.seconds - src.seconds), (dst.sequence_number - src.sequence_number), CAST((dst.seconds - src.seconds) AS FLOAT) / (dst.sequence_number - src.sequence_number)
            FROM raw_readings_t AS src
            CROSS JOIN raw_readings_t AS dst
            WHERE src.tess == dst.tess
            AND   dst.id == src.id - 1
            AND   src.tess = "stars84"
            AND   src.date_id == 20190824;
            ''')
        connection.commit()

def daily(connection, name, date_ide, n):
    row = {'name': name, 'date_id': date_id}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT tess, date_id, time_id, id
        FROM  raw_readings_t
        WHERE tess = :name
        AND   date_id = :date_id
        ''', row)
    return cursor

def day_differences(connection, daily_iterable):
    row = {}
    for prev, cur in pairs_generator(daily_iterable):
        row['deltaSeq'] = cur[0] - prev[0]
        row['deltaT']   = cur[1] - prev[1]
        row['period']   = float(row['deltaT'])/row['deltaSeq']
        cursor = connection.cursor()
        cursor.execute(
            '''
            INSERT INTO first_differences_t(tess, date_id, time_id, id, seq_diff, seconds_diff, period)
            VALUES(
                :name,
                :date_id,
                :time_id,
                :id,
                :deltaSeq,
                :deltaT,
                :period
            )
            ''', row)
        connection.commit()


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
            cursor.execute(
            '''
            INSERT INTO raw_readings_t(id, date_id, time_id, tess, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength, seconds) 
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ''', row)
        except sqlite3.IntegrityError as e:
            duplicates[row[3]] = duplicates.get(row[3],0) + 1
            counter = factory.build(row[3])
            c = counter.prev()
            logging.debug("[{0}] Duplicated row, restoring counter for {1} to {2}".format(__name__, row[3], c))
        else:
            temp = max_date_id.get(row[3],0)
            max_date_id[row[3]] = row[1] if row[1] > temp else temp
    factory.saveMax(max_date_id)
    connection.commit()
    logging.info("[{0}] Duplicates summary: {1}".format(__name__, duplicates))
    #paging(cursor,["TESS","MAC","Site"])


def input_stats(connection, options):
    cursor = connection.cursor()
    logging.info("[{0}] Starting Tx period stats calculation".format(__name__))
    kk(connection, by_name_and_day(connection))

   




