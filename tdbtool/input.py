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


# Access  template withing the package
from pkg_resources import resource_filename

# -------------
# Local imports
# -------------

import tdbtool.s4a as s4a
from .      import __version__
from .utils import paging

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------



# --------------
# Module classes
# --------------

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


    def findMax(self, name):
        cursor = self._connection.cursor()
        row = {'name': name}
        try:
            cursor.execute(
                '''
                SELECT MAX(id) FROM raw_readings_t
                WHERE tess == :name
                ''', row)
        except Exception as e:
            logging.info("[{0}] table does not exist".format(__name__))
            print(e)
            result = -1
        else:
            result = cursor.fetchone()[0]
        return result if result is not None else -1


    def build(self, name):
        if name not in self._pool.keys():
            value = self.findMax(name)
            c = Counter(value+1)
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
            dateid, timeid = s4a.datetime.from_iso8601(srcrow[0]).dbase_ids()
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
            try:
                val = int(srcrow[7])
            except Exception as e:
                val = None
            row.append(val)    # RSS
            counter.next()  
            yield row



def input_slurp(connection, options):
    duplicates = {}
    cursor = connection.cursor()
    factory = CounterFactory(connection)
    logging.info("[{0}] Starting ingestion from {1}".format(__name__, options.csv_file))
    for row in csv_generator(options.csv_file, factory):
        try:
            cursor.execute(
            '''
            INSERT INTO raw_readings_t(id, date_id, time_id, tess, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength) 
            VALUES (?,?,?,?,?,?,?,?,?,?)
            ''', row)
        except sqlite3.IntegrityError as e:
            duplicates[row[3]] = duplicates.get(row[3],0) + 1
            counter = factory.build(row[3])
            c = counter.prev()
            logging.debug("[{0}] Duplicated row, restoring counter for {1} to {2}".format(__name__, row[3], c))
    connection.commit()
    logging.info("[{0}] Duplicates summary: {1}".format(__name__, duplicates))
    #paging(cursor,["TESS","MAC","Site"])




