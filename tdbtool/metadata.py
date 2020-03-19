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

# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__
from .      import BEFORE
from .utils import open_database

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

FLAGS_SUBSCRIBER_IMPORTED = 2

SQLITE_REGEXP_MODULE = "/usr/lib/sqlite3/pcre.so"

# -----------------------
# Module global variables
# -----------------------

def good_readings_iterable(connection, name):
    cursor = connection.cursor()
    if name is None:
        cursor.execute(
            '''
            SELECT name, date_id, time_id
            FROM  raw_readings_t 
            WHERE rejected IS NULL
            ORDER BY name ASC, date_id ASC, time_id ASC
            ''')
    else:
        row = {'name': name}
        cursor.execute(
            '''
            SELECT name, date_id, time_id
            FROM  raw_readings_t
            WHERE rejected IS NULL 
            AND name == :name
            ORDER BY name ASC, date_id ASC, time_id ASC
            ''', row)
    return cursor


def open_reference_database(path):
    connection = open_database(path)
    connection.enable_load_extension(True)
    connection.load_extension(SQLITE_REGEXP_MODULE)
    return connection


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


def _get_tess_id(connection, mac_address, tstamp):
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
        return {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': BEFORE}
    tess_id = _get_tess_id(connection, mac, tstamp)
    return {'name': name, 'date_id': date_id, 'time_id': time_id,'tess_id': tess_id}


def mark_before_registry(connection, bad_rows):
    logging.info("[{0}] marking {1} bad rows detected before photometer registration".format(__name__, len(bad_rows)))
    cursor = connection.cursor()
    cursor.executemany('''
        UPDATE raw_readings_t
        SET rejected = :reason
        WHERE name  == :name
        AND date_id == :date_id
        AND time_id == :time_id
        ''', bad_rows)
    connection.commit()


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

# ==============
# MAIN FUNCTIONS
# ==============

def metadata_flags(connection, options):
    cursor = connection.cursor()
    if options.name is None:
        logging.info("[{0}] setting flags metadata for all = 0x{1:02X}".format(__name__, FLAGS_SUBSCRIBER_IMPORTED))
        row = {'value': FLAGS_SUBSCRIBER_IMPORTED}
        cursor.execute(
            '''
            UPDATE raw_readings_t
            SET units_id = :value
            WHERE rejected is NULL
            ''', row)
    else:
        logging.info("[{0}] setting flags metadata to {1} = 0x{2:02X}".format(__name__, options.name, FLAGS_SUBSCRIBER_IMPORTED))
        row = {'name': options.name, 'value': FLAGS_SUBSCRIBER_IMPORTED}
        cursor.execute(
             '''
            UPDATE raw_readings_t
            SET units_id = :value
            WHERE rejected is NULL
            AND name == :name
            ''', row)
    connection.commit()
    logging.info("[{0}] Done!".format(__name__))


def metadata_location(connection, options):
    logging.info("[{0}] adding location metadata".format(__name__))
    logging.info("[{0}] Opening reference database {1}".format(__name__,options.dbase))
    connection2 = open_reference_database(options.dbase)
    logging.info("[{0}] Done!".format(__name__))

 
def metadata_instrument(connection, options):
    tess_ids = []
    bad_rows = []
    count = 0
    logging.info("[{0}] adding instrument metadata".format(__name__))
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    for row in good_readings_iterable(connection, options.name):
        tess_id = get_tess_id(connection2, row[0], row[1], row[2])
        if 'reason' in tess_id:
            bad_rows.append(tess_id)
            continue
        if len(tess_ids)  < ROWS_PER_COMMIT:
            tess_ids.append(tess_id)
        else:
            count += ROWS_PER_COMMIT
            update_tess_id(connection, tess_ids)
            tess_ids = []
    
    if len(tess_ids):
        count += len(tess_ids)
        update_tess_id(connection, tess_ids)

    if len(bad_rows):
        mark_before_registry(connection, bad_rows)
    logging.info("[{0}] Updated {1} tess ids. Done!".format(__name__, count))


