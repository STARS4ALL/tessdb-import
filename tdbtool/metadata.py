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
from .utils import open_database, candidate_names_iterable

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

FLAGS_SUBSCRIBER_IMPORTED = 2

SQLITE_REGEXP_MODULE = "/usr/lib/sqlite3/pcre.so"

# -----------------------
# Module global variables
# -----------------------

def open_reference_database(path):
    connection = open_database(path)
    connection.enable_load_extension(True)
    connection.load_extension(SQLITE_REGEXP_MODULE)
    return connection


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
        ORDER BY name ASC, date_id ASC, time_id ASC
        ''', row)
    return cursor


def good_readings_iterable2(connection, name):
    '''Used to find out location_id values'''
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT date_id, time_id, tess_id
        FROM  raw_readings_t
        WHERE rejected IS NULL
        AND tess_id IS NOT NULL 
        AND name == :name
        ORDER BY name ASC, date_id ASC, time_id ASC
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
        result = {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': BEFORE}
    else:
        tess_id =_get_tess_id(connection, mac, tstamp)
        result = {'name': name, 'date_id': date_id, 'time_id': time_id,'tess_id': tess_id}
    return result

#####################################

def express_find_location_id(connection, tess_id, date_id):
    row = {'tess_id': tess_id, 'date_id': date_id}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT location_id
        FROM location_daily_aggregate_t
        WHERE tess_id == :tess_id
        AND   date_id == :date_id
        AND   same_location == 1
        ''', row)
    return cursor.fetchall()
   

def slow_find_location_id(connection, tess_id, tstamp, period):
    row = {'tess_id': tess_id, 'tstamp': tstamp}
    row['high'] = str(period/2) + ' seconds'
    row['low'] = str(-period/2) + ' seconds'
    cursor = connection.cursor()
    cursor.execute('''
        SELECT r.location_id
        FROM tess_readings_t AS r
        JOIN date_t AS d USING (date_id)
        JOIN time_t AS t USING (time_id)
        WHERE r.tess_id == :tess_id
        AND datetime(d.sql_date || 'T' || t.time) 
        BETWEEN datetime(:tstamp, :low) 
        AND datetime(:tstamp, :high)
        ''', row)
    return cursor.fetchall()


def find_location_id(connection, connection2, tess_id, date_id, time_id, period):
    location_id = express_find_location_id(connection, tess_id, date_id)
    if len(location_id) == 0 :
        tstamp = tdbtool.s4a.datetime.from_dbase_ids(date_id, time_id).to_iso8601()
        location_id = slow_find_location_id(connection2, tess_id, tstamp, period)
    return location_id


def get_period(connection, name, date_id):
    row = {'name': name, 'date_id': date_id}
    cursor = connection.cursor()
    cursor.execute('''
        SELECT median_period
        FROM daily_stats_t
        WHERE name == :name
        AND date_id == :date_id
        ''', row)
    return cursor.fetchone()[0]


def get_location_id(connection, connection2, name, date_id, time_id, tess_id):
    period = get_period(connection, name, date_id)
    logging.info("[{0}] {1} ({5}) period    for {2}T{3:06d} is  {4}".format(__name__, name, date_id, time_id, period, tess_id))
    location_ids = find_location_id(connection, connection2, tess_id, date_id, time_id, period)
    logging.info("[{0}] {1} ({5}) locations for {2}T{3:06d} are {4}".format(__name__, name, date_id, time_id, location_ids,tess_id))
    result = {'name': name, 'date_id': date_id, 'time_id': time_id, 'reason': "NO IMPLEMENTADO"}
    return result

######################################3

def mark_bad_rows(connection, bad_rows):
    name = bad_rows[0]['name']
    logging.info("[{0}] marking {2} bard rows for {1}".format(__name__, name, len(bad_rows)))
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
    
    if len(tess_ids):
        count += len(tess_ids)
        update_tess_id(connection, tess_ids)

    if len(bad_rows):
        mark_bad_rows(connection, bad_rows)
    logging.info("[{0}] Updated {1} tess ids for {2}.".format(__name__, count, name))


def metadata_location_by_name(connection, name, connection2):
    location_ids = []
    bad_rows = []
    count    = 0
    logging.debug("[{0}] adding instrument metadata to {1}".format(__name__, name))
    for row in good_readings_iterable2(connection, name):
        location_id = get_location_id(connection, connection2, name, row[0], row[1], row[2])
        if 'reason' in location_id:
            #bad_rows.append(location_id)
            continue
        if len(location_ids)  < ROWS_PER_COMMIT:
            location_ids.append(location_id)
        else:
            count += ROWS_PER_COMMIT
            update_location_id(connection, location_ids)
            location_ids = []
    
    if len(location_ids):
        count += len(location_ids)
        update_location_id(connection, location_ids)

    if len(bad_rows):
        mark_bad_rows(connection, bad_rows)
    logging.info("[{0}] Updated {1} tess ids for {2}.".format(__name__, count, name))


def aggregates_iterable(connection):
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT tess_id, date_id, MIN(location_id), (MIN(location_id) == MAX(location_id))
        FROM tess_readings_t
        GROUP BY tess_id, date_id
        ''')
    return cursor

def aggregates_update(connection, iterable):
    cursor = connection.cursor()
    cursor.executemany(
        '''
        INSERT OR REPLACE INTO location_daily_aggregate_t(tess_id, date_id, location_id, same_location)
        VALUES(?,?,?,?)
        ''',iterable)
    connection.commit()

def metadata_refresh_all(connection, connection2):
    logging.info("[{0}] Refresing metadata from reference database".format(__name__))
    aggregates_update(connection, aggregates_iterable(connection2))
    logging.info("[{0}] Done!".format(__name__))


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


def metadata_instrument(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    if options.name is not None:
        metadata_instrument_by_name(connection, options.name, connection2)
    else:
        for name in candidate_names_iterable(connection):
            metadata_instrument_by_name(connection, name[0], connection2)


def metadata_location(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    if options.name is not None:
        metadata_location_by_name(connection, options.name, connection2)
    else:
        for name in candidate_names_iterable(connection):
            metadata_location_by_name(connection, name[0], connection2)

def metadata_refresh(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    metadata_refresh_all(connection, connection2)
   
