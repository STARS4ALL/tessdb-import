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

# -----------------------
# Module global functions
# -----------------------


def csv_generator(filepath):
    '''An iterator that reads csv line by line and keeps memory usage down'''
    with open(filepath, "r") as csvfile:
        datareader = csv.reader(csvfile,delimiter=';')
        dummy = next(datareader)  # drops the header row
        counter = 0
        for srcrow in datareader:
            row = []
            counter += 1
            dateid, timeid = s4a.datetime.from_iso8601(srcrow[0]).dbase_ids()
            row.append(counter)
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
            yield row



def input_slurp(connection, options):
    cursor = connection.cursor()
    try:
        cursor.executemany(
        '''
        INSERT OR IGNORE INTO raw_readings_t(id, date_id, time_id, tess, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength) 
        VALUES (?, ?,?,?,?,?,?,?,?,?)
        ''', iterable)
    except Exception as e:
        print(e)
    connection.commit()
    #paging(cursor,["TESS","MAC","Site"])




