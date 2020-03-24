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
from .      import BEFORE
from .utils import open_database, open_reference_database, mark_bad_rows
from .utils import candidate_names_iterable, shift_generator, get_period
# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

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
    '''Used to find out tess_id values'''
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT date_id, time_id
        FROM  raw_readings_t
        WHERE rejected IS NULL 
        AND name == :name
        ORDER BY date_id ASC, time_id ASC
        ''', row)
    return cursor


def get_mac(connection, name, tstamp):
    '''Get the photometer MAC address at a given point in time'''
    row = {'name': name, 'tstamp': tstamp}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT mac_address
        FROM name_to_mac_t
        WHERE name == :name
        AND datetime(:tstamp) BETWEEN datetime(valid_since) AND datetime(valid_until)
        ''', row)
    result = cursor.fetchone()
    if result is not None:
        result = result[0]
    return result


def find_tess_id(connection, mac_address, tstamp):
    row = {'mac': mac_address, 'tstamp': tstamp}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT tess_id
        FROM tess_t
        WHERE mac_address == :mac
        AND datetime(:tstamp) BETWEEN datetime(valid_since) AND datetime(valid_until)
        ''', row)
    return cursor.fetchone()[0]


def get_tess_id(connection, name, date_id, time_id):
    tstamp = tdbtool.s4a.datetime.from_dbase_ids(date_id, time_id).to_iso8601()
    mac = get_mac(connection, name, tstamp)
    if mac is None:
        result = {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': BEFORE}
    else:
        tess_id = find_tess_id(connection, mac, tstamp)
        result = {'name': name, 'date_id': date_id, 'time_id': time_id,'tess_id': tess_id}
    return result



def update_tess_id(connection, iterable):
    cursor = connection.cursor()
    cursor.executemany('''
        UPDATE raw_readings_t
        SET tess_id =  :tess_id
        WHERE name  == :name
        AND date_id == :date_id
        AND time_id == :time_id
        ''', iterable)
    connection.commit()



def metadata_instrument_by_name(connection, name, connection2):
    tess_ids = []
    bad_rows = []
    count    = 0
    logging.debug("[{0}] adding instrument metadata to {1}".format(__name__, name))
    for row in good_readings_iterable(connection, name):
        tess_id = get_tess_id(connection2, name, row[0], row[1])
        if 'reason' in tess_id:
            bad_rows.append(tess_id)
            continue
        if len(tess_ids)  < ROWS_PER_COMMIT:
            tess_ids.append(tess_id)
        else:
            count += ROWS_PER_COMMIT
            update_tess_id(connection, tess_ids)
            tess_ids = []
            logging.info("[{0}] Updated {1} instrument metadata until {2}".format(__name__, name, row[0]))
    if len(tess_ids):
        count += len(tess_ids)
        update_tess_id(connection, tess_ids)

    if len(bad_rows):
        mark_bad_rows(connection, bad_rows)
    logging.info("[{0}] Updated {1} tess ids for {2}.".format(__name__, count, name))



# ==============
# MAIN FUNCTIONS
# ==============


def metadata_instrument(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    if options.name is not None:
        metadata_instrument_by_name(connection, options.name, connection2)
    else:
        for name in candidate_names_iterable(connection):
            metadata_instrument_by_name(connection, name[0], connection2)

