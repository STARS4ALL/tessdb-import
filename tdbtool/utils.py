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

import os
import os.path
import sys
import collections
import sqlite3

# Python3 catch
try:
    raw_input
except:
    raw_input = input 

#--------------
# other imports
# -------------

import tabulate

#--------------
# local imports
# -------------

from .s4a import datetime

# ----------------
# Module constants
# ----------------

SQLITE_REGEXP_MODULE = "/usr/lib/sqlite3/pcre.so"
SQLITE_MATH_MODULE   = "/usr/local/lib/libsqlitefunctions.so"

# ----------------
# package constants
# ----------------


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


def mkdate(datestr):
    date = none
    for fmt in ['%Y-%m','%Y-%m-%d','%Y-%m-%dT%H:%M:%S','%Y-%m-%dT%H:%M:%SZ']:
        try:
            date = datetime.strptime(datestr, fmt)
        except ValueError:
            pass
    return date


def utf8(s):
    if sys.version_info[0] < 3:
        return unicode(s, 'utf8')
    else:
        return (s)

def percent(n):
   n = max(n,0)
   n = min(n,100)
   return n

def shift_generator(iterable, N):
    '''Partitions a very long iterable in tuples of size N shifting one item at every ietration'''
    q = collections.deque((None for x in range(0,N)), N)
    for current in iterable:
        q.append(current)
        yield tuple(x for x in q)


if sys.version_info[0] < 3:
    # Python 2 version
    def packet_generator(iterable, size):
        '''Generates a sequence of 'size' items from an iterable'''
        finished = False
        while not finished:
            acc = []
            for i in range(0,size):
                try:
                    obj = iterable.next()
                except AttributeError:
                    iterable = iter(iterable)
                    obj = iterable.next()
                    acc.append(obj)
                except StopIteration:
                    finished = True
                    break
                else:
                    acc.append(obj)
            if len(acc):
                yield acc
else:
    # Python 3 version
    def packet_generator(iterable, size):
        '''Generates a sequence of 'size' items from an iterable'''
        finished = False
        while not finished:
            acc = []
            for i in range(0,size):
                try:
                    obj = iterable.__next__()
                except AttributeError:
                    iterable = iter(iterable)
                    obj = iterable.__next__()
                    acc.append(obj)
                except StopIteration:
                    finished = True
                    break
                else:
                    acc.append(obj)
            if len(acc):
                yield acc


def paging(iterable, headers, maxsize=10, page_size=10):
    '''
    Pages query output and displays in tabular format
    '''
    for rows in packet_generator(iterable, page_size):
        print(tabulate.tabulate(rows, headers=headers, tablefmt='grid'))
        maxsize -= page_size
        if len(rows) == page_size and maxsize > 0:
            raw_input("Press <Enter> to continue or [Ctrl-C] to abort ...")
        else:
            break

# ==============
# DATABASE STUFF
# ==============


def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    return sqlite3.connect(dbase_path)

def open_reference_database(path):
    connection = open_database(path)
    connection.enable_load_extension(True)
    connection.load_extension(SQLITE_REGEXP_MODULE)
    connection.load_extension(SQLITE_MATH_MODULE)
    return connection


def update_rejection_code(connection, bad_rows):
    name = bad_rows[0]['name']
    cursor = connection.cursor()
    cursor.executemany('''
        UPDATE raw_readings_t
        SET rejected = :reason
        WHERE name  == :name
        AND date_id == :date_id
        AND time_id == :time_id
        ''', bad_rows)
    connection.commit()


def candidate_names_iterable(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT DISTINCT name
        FROM raw_readings_t 
        ORDER BY name ASC
        ''')
    return cursor

def previous_iterable(connection, iterable):
    result = []
    for srcrow in iterable:
        row = {'rank': srcrow[0], 'name': srcrow[3]}
        cursor = connection.cursor()
        cursor.execute(
            '''
            SELECT r.rank, r.rejected, r.tstamp, r.name, r.sequence_number, r.frequency, r.magnitude, r.ambient_temperature, r.sky_temperature, r.signal_strength
            FROM raw_readings_t AS r
            WHERE r.rank == :rank - 1
            AND   r.name == :name
            AND   r.rejected IS NULL
            ''', row)
        temp = cursor.fetchone()
        if temp is not None:
            result.append(temp)
    return result
