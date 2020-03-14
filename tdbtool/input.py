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
from .utils import tuple_generator

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



# --------------
# Module classes
# --------------
class datetime(tdbtool.s4a.datetime):


    def to_seconds(self):
        '''
        Return date and time database identifiers + seconds within the day
        '''
        return 3600*self.hour + 60*self.minute + self.second

    @classmethod
    def from_integer_iso8601(cls, tstamp):
        '''Converts from an integer YYYYMMDDHHMMSS'''
        date = tstamp // 1000000
        time = tstamp %  1000000
        yyyy = date // 10000
        mmmm = (date % 10000) // 100
        dd   = (date % 10000) %  100
        hh   = time // 10000
        mm   = (time % 10000) // 100
        ss   = (time % 10000) %  100
        return cls(yyyy, mmmm, dd, hh, mm, ss)



class Counter(object):
    '''A counter to inject in database'''

    def __init__(self, value, max_tstamp, persisted):
        self._value = value
        self._max_tstamp = max_tstamp
        self._already_persisted = persisted
    def next(self):
        self._value += 1
        return self._value

    def prev(self):
        self._value -= 1
        return self._value

    def current(self):
        return self._value

    # These two methods manage the maximun timestamp per TESS-W read in the input CSV file
    def update_tstamp(self, tstamp):
        self._max_tstamp = tstamp if tstamp > self._max_tstamp else self._max_tstamp

    def max_tstamp(self):
        return self._max_tstamp

    def persisted(self):
        return self._already_persisted 



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
                SELECT max_rank, max_tstamp FROM housekeeping_t
                WHERE name == :name
                ''', row)
        except Exception as e:
            logging.info("[{0}] table does not exist".format(__name__))
            row['max_rank']   = 0
            row['max_tstamp'] = 0 
        else:
            result = cursor.fetchone()
            if result is not None:
                row['max_rank']   = result[0]
                row['max_tstamp'] = result[1]
            else:
                row['max_rank']    = 0
                row['max_tstamp']  = 0
            logging.info("[{0}] Loading Counters {1}".format(__name__, row))
        return row['max_rank'], row['max_tstamp'], row['max_tstamp'] != 0


    def saveMax(self):
        cursor = self._connection.cursor()
        for key in self._pool.keys():
            try:
                row = {
                    'name': key, 
                    'max_rank': self._pool[key].current()-1,
                    'max_tstamp' : self._pool[key].max_tstamp(),
                }
                cursor.execute(
                    '''
                    INSERT OR REPLACE INTO housekeeping_t(name, max_rank, max_tstamp) 
                    VALUES (:name, :max_rank, :max_tstamp)
                    ''', row)
                logging.info("[{0}] Saving counters {1}".format(__name__, row))
            except Exception as e:
                logging.error("[{0}] Error saving counters".format(__name__))
                traceback.print_exc()
        


    def build(self, name):
        if name not in self._pool.keys():
            max_rank, max_tstamp, persisted = self.loadMax(name)
            c = Counter(max_rank+1, max_tstamp, persisted)
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
        line_number = 2
        for srcrow in datareader:
            row = []
            dt = datetime.from_iso8601(srcrow[0], TSTAMP_FORMAT)
            #dateid, timeid, seconds = datetime.from_iso8601(srcrow[0],TSTAMP_FORMAT).to_dbase_ids()
            date_id, time_id = dt.to_dbase_ids()   
            counter = factory.build(srcrow[1])
            row.append(counter.current())  # rank = 0
            row.append(date_id)            # date_id = 1
            row.append(time_id)            # time_id = 2
            row.append(srcrow[1])          # name = 3
            row.append(int(srcrow[2]))     # sequence number = 4
            row.append(float(srcrow[3]))   # frequency = 5
            row.append(float(srcrow[4]))   # magnitude = 6
            row.append(float(srcrow[5]))   # tamb = 7
            row.append(float(srcrow[6]))   # tsky = 8
            row.append(dt.to_seconds())    # number of seconds within the day = 9
            try:
                val = int(srcrow[7])
            except Exception as e:
                val = None
            row.append(val)                # RSS  = 10
            row.append(date_id*1000000+time_id) # combined integer timestamp = 11
            row.append(line_number)             # original file line number  = 12
            line_number += 1
            yield row


def name_and_date_iterable(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT  name, date_id, count(*)
        FROM    raw_readings_t
        GROUP BY name, date_id
        ORDER BY name ASC, date_id ASC
        ''')
    return cursor   # return Cursor as an iterable


def daily_iterable(connection, name, date_id):
    row = {'name': name, 'date_id': date_id}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT time_id, seconds, sequence_number, rank
        FROM   raw_readings_t
        WHERE  name = :name
        AND    date_id = :date_id
        ''', row)
    return cursor   # return Cursor as an iterable


def compute_daily_differences(name, date_id, prev, cur, N):
    row = {
            'name'    : name,
            'date_id' : date_id,
            'time_id' : cur[0],
            'deltaT'  : cur[1] - prev[1],
            'deltaSeq': cur[2] - prev[2],
            'rank'    : cur[3],
            'N'       : N,
            'seqno'   : cur[2],
            'ctrl'    : cur[3] - prev[3]
        }
    try:
        row['period']  = float(cur[1] - prev[1])/(cur[2] - prev[2])
    except ZeroDivisionError:
        logging.debug("[{0}] {1}: on {2}-{3:06d}. Sequence number issue between {4} and {5}".format(__name__, name, date_id, cur[0], prev, cur))
    finally:
        return row

def write_daily_differences(connection, iterable):
    logging.debug("[{0}] Wriiting differences".format(__name__))
    cursor = connection.cursor()
    cursor.executemany(
        '''
        INSERT OR IGNORE INTO first_differences_t(name, date_id, time_id, rank, seq_diff, seconds_diff, period, N, control)
        VALUES(
            :name,
            :date_id,
            :time_id,
            :rank,
            :deltaSeq,
            :deltaT,
            :period,
            :N,
            :ctrl
        )
         ''', iterable)
    connection.commit()


def mark_duplicated_seqno(connection, row):
    '''Marks the most recent rows with duplicated sequence number'''
    row['reason'] = DUP_SEQ_NUMBER
    cursor = connection.cursor()
    cursor.execute(
        '''
        UPDATE raw_readings_t
        SET    rejected = :reason
        WHERE  name            == :name
        AND    date_id         == :date_id
        -- AND    time_id         == :time_id
        AND    sequence_number == :seqno
         ''', row)
    # Let the global commit do it


def mark_corner_cases(connection, name, date_id, N):
    '''Marks daily readings for a given TESS-W when N is 1 or 2'''
    if N > 2:
        return
    row = {'name': name, 'date_id': date_id}
    if N == 1:
        row['reason'] = SINGLE
    else:
        row['reason'] = PAIR
    cursor = connection.cursor()
    cursor.execute(
        '''
        UPDATE raw_readings_t
        SET    rejected = :reason
        WHERE  name            == :name
        AND    date_id         == :date_id
         ''', row)
    # Let the global commit do it

def mark_duplicated_tstamp(connection, row, file_name):
    '''Marks both rows with duplicated sequence num bers'''
    cursor = connection.cursor()
    cursor.execute(
        '''
        INSERT OR IGNORE INTO duplicated_readings_t(rank, date_id, time_id, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, seconds, signal_strength, tstamp, line_number) 
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ''', row)
    row2 = { 'name': row[3], 'date_id': row[1], 'time_id': row[2], 'file': file_name}
    row2['iso8601'] = datetime.from_integer_iso8601(row[11]).to_iso8601(TSTAMP_FORMAT)
    cursor.execute(
        '''
        UPDATE duplicated_readings_t
        SET file = :file, iso8601 = :iso8601 
        WHERE  name            == :name
        AND    date_id         == :date_id
        AND    time_id         == :time_id
        ''', row2)
    # Let the global commit do it

# ==============
# MAIN FUNCTIONS
# ==============


def input_slurp(connection, options):
    logging.info("[{0}] Starting ingestion from {1}".format(__name__, options.csv_file))
    duplicates = {}
    cursor = connection.cursor()
    factory = CounterFactory(connection)
    for row in csv_generator(options.csv_file, factory):
        counter = factory.build(row[3])
        if row[11] < counter.max_tstamp():
            # Skip old data
            continue
        else:
            counter.next()
            try:
                cursor.execute(
                    '''
                    INSERT INTO raw_readings_t(rank, date_id, time_id, name, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, seconds, signal_strength, tstamp, line_number) 
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    ''', row)
            except sqlite3.IntegrityError as e:
                if row[11] == counter.max_tstamp() and not counter.persisted():
                    mark_duplicated_tstamp(connection, row, options.csv_file)
                    duplicates[row[3]] = duplicates.get(row[3],0) + 1
                oldv = counter.prev()
                logging.debug("[{0}] Duplicated row on {3}, restoring counter for {1} to {2}".format(__name__, row[3], oldv, row[11]))
            else:
                counter.update_tstamp(row[11])
    logging.info("[{0}] Ended ingestion from {1}".format(__name__, options.csv_file))
    logging.info("[{0}] Saving housekeeping data".format(__name__))
    factory.saveMax()
    connection.commit()
    logging.info("[{0}] Duplicates summary: {1}".format(__name__, duplicates))

                
def input_differences(connection, options):
    cursor = connection.cursor()
    logging.info("[{0}] Starting Tx period stats calculation".format(__name__))
    rows = []
    for group in name_and_date_iterable(connection):
        name    = group[0]
        date_id = group[1]
        N       = group[2]
        mark_corner_cases(connection, name, date_id, N)
        logging.info("[{0}] Computing differences for {1} on {2} ({3} points)".format(__name__, name, date_id, N))
        for points in tuple_generator(daily_iterable(connection, name, date_id), 2):
            if not all(points):
                continue
            else:
                prev, cur = points
                if len(rows) < 10000:
                    row = compute_daily_differences(name, date_id, prev, cur, N)
                    if 'period' in row:
                        rows.append(row)
                    else:
                        mark_duplicated_seqno(connection, row)
                else:
                    write_daily_differences(connection, rows)
                    rows = []
    logging.info("[{0}] Done!".format(__name__))

                


   


   

   




