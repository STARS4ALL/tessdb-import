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
from .      import AMBIGUOUS_LOC
from .utils import open_database, open_reference_database, mark_bad_rows
from .utils import candidate_names_iterable, shift_generator, get_period

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

TEMP_REJECTED_LOCATION_ID = -100

# -----------------------
# Module global variables
# -----------------------

gap_list = {}

# --------------
# Module classes
# --------------

class LocationGap(object):

    def __init__(self, connection, name, connection2):
        self._connection  = connection
        self._connection2 = connection2
        self._name        = name


    def markStart(self, date_id, time_id, location_id):
        self._start_date_id = date_id
        self._start_time_id = time_id
        self._start_loc_id  = location_id


    def markEnd(self, date_id, time_id, location_id):
        self._end_date_id = date_id
        self._end_time_id = time_id
        self._end_loc_id  = location_id

   
    def _updateLocation(self, row):
        cursor = self._connection.cursor()
        cursor.execute(
            '''
            UPDATE raw_readings_t
            SET location_id = :new_location_id, rejected = :reason
            WHERE location_id == :old_location_id
            AND name == :name
            AND datetime(trim(tstamp,'Z'))
            BETWEEN datetime(:low)
            AND datetime(:high)
            ''', row)

    def getCount(self, row):
        cursor = self._connection.cursor()
        cursor.execute(
                '''
                SELECT COUNT(*)
                FROM raw_readings_t
                WHERE location_id == :old_location_id
                AND name == :name
                AND datetime(trim(tstamp,'Z'))
                BETWEEN datetime(:low)
                AND datetime(:high)
                ''', row)
        return cursor.fetchone()[0]


    def updateLocation(self, row):
        row['new_location_id'] = self._start_loc_id
        row['reason'] = None
        logging.info("[{0}] fixed {1} readings for {2} to location id {3} between {4} and {5}".format(__name__, row['count'], row['name'], row['new_location_id'], row['low'], row['high']))
        self._updateLocation(row)


    def rollbackLocation(self, row):
        row['new_location_id'] = None
        row['reason'] = AMBIGUOUS_LOC
        logging.warning("[{0}] can't fix location id for {1} between {2} and {3} ({4}) readings".format(__name__, row['name'], row['low'], row['high'], row['count']))
        self._updateLocation(row)

    def setLocationNames(self, row):
        cursor2 = self._connection2.cursor()
        cursor2.execute(
            '''
            SELECT site
            FROM location_t
            WHERE location_id == :start_loc_id
            ''', row)
        row['start_site'] = cursor2.fetchone()[0]
        cursor2.execute(
            '''
            SELECT site
            FROM location_t
            WHERE location_id == :end_loc_id
            ''', row)
        row['end_site'] = cursor2.fetchone()[0]
        logging.warning("[{0}] mismatching location for {1} are {2} and {3} respectively".format(__name__, row['name'], row['start_site'].encode('utf-8'), row['end_site'].encode('utf-8')))
        cursor = self._connection.cursor()
        cursor.execute(
            '''
            UPDATE location_gaps_t
            SET start_location = :start_site, end_location = :end_site
            WHERE name == :name
            AND start_date_id = :start_date_id
            AND start_time_id = :start_time_id
            AND end_date_id   = :end_date_id
            AND end_time_id   = :end_time_id
            ''', row)


    def logToDbase(self, row):
        row['start_date_id'] = self._start_date_id
        row['start_time_id'] = self._start_time_id
        row['end_date_id']   = self._end_date_id
        row['end_time_id']   = self._end_time_id
        row['start_loc_id']  = self._start_loc_id
        row['end_loc_id']    = self._end_loc_id
        logging.warning("[{0}] mismatching location ids for {1} are {2} and {3} respectively".format(__name__, row['name'], row['start_loc_id'], row['end_loc_id']))
        cursor = self._connection.cursor()
        cursor.execute(
            '''
            INSERT OR REPLACE INTO location_gaps_t (
                name,
                start_date_id,
                start_time_id,
                start_tstamp,
                start_location_id,
                end_date_id,
                end_time_id,
                end_tstamp,
                end_location_id,
                readings 
                ) VALUES (
                :name,
                :start_date_id,
                :start_time_id,
                :low,
                :start_loc_id,
                :end_date_id,
                :end_time_id,
                :high,
                :end_loc_id,
                :count
                )
            ''', row)
        self.setLocationNames(row)


    def fix(self):
        row = {
                
                'old_location_id': TEMP_REJECTED_LOCATION_ID,
                'name': self._name,
                'low' : tdbtool.s4a.datetime.from_dbase_ids(self._start_date_id, self._start_time_id).to_iso8601(),
                'high': tdbtool.s4a.datetime.from_dbase_ids(self._end_date_id, self._end_time_id).to_iso8601(),
            }
        row['count'] = self.getCount(row)
        if self._start_loc_id == self._end_loc_id:
            self.updateLocation(row)
        else:
            self.logToDbase(row)
            self.rollbackLocation(row)
        self._connection.commit()


# -----------------------
# Module global functions
# -----------------------


def good_readings_iterable(connection, name):
    '''Used to find out location_id values'''
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT date_id, time_id, tess_id
        FROM  raw_readings_t
        WHERE rejected IS NULL
        AND tess_id IS NOT NULL
        AND location_id IS NULL 
        AND name == :name
        ORDER BY date_id ASC, time_id ASC
        ''', row)
    return cursor


def good_readings_iterable2(connection, name):
    '''Used to detect gaps in location_id values'''
    row = {'name': name}
    cursor = connection.cursor()
    cursor.execute(
        '''
        SELECT date_id, time_id, location_id
        FROM  raw_readings_t
        WHERE rejected IS NULL
        AND tess_id IS NOT NULL
        AND location_id IS NOT NULL 
        AND name == :name
        ORDER BY date_id ASC, time_id ASC
        ''', row)
    return cursor


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
    return cursor.fetchone()
   

def slow_find_location_id(connection, tess_id, tstamp, period):
    row = {'tess_id': tess_id, 'tstamp': tstamp}
    row['high'] = str(period/2)  + ' seconds'
    row['low']  = str(-period/2) + ' seconds'
    cursor = connection.cursor()
    cursor.execute('''
        SELECT location_id
        FROM tess_readings_t AS r
        WHERE tess_id == :tess_id
        AND datetime(iso8601fromids(date_id, time_id)) 
        BETWEEN datetime(:tstamp, :low) 
        AND datetime(:tstamp, :high)
        ''', row)
    return cursor.fetchone()


def find_location_id(connection, connection2, tess_id, date_id, time_id, period):
    location_id = express_find_location_id(connection, tess_id, date_id)
    if location_id is None :
        tstamp = tdbtool.s4a.datetime.from_dbase_ids(date_id, time_id).to_iso8601()
        location_id = slow_find_location_id(connection2, tess_id, tstamp, period)
    return location_id


def get_location_id(connection, connection2, name, date_id, time_id, tess_id):
    period = get_period(connection, name, date_id)
    location_id = find_location_id(connection, connection2, tess_id, date_id, time_id, period)
    if location_id is None:
        location_id = TEMP_REJECTED_LOCATION_ID
    else:
        location_id = location_id[0]
    logging.debug("[{0}] {1} ({2}) location for {3}T{4:06d} using period {5} is {6}".format(__name__, name, tess_id, date_id, time_id, period, location_id))
    result = {'name': name, 'date_id': date_id, 'time_id': time_id, 'location_id': location_id}
    return result


def update_location_id(connection, iterable):
    cursor = connection.cursor()
    cursor.executemany('''
        UPDATE raw_readings_t
        SET location_id = :location_id
        WHERE name  == :name
        AND date_id == :date_id
        AND time_id == :time_id
        ''', iterable)
    connection.commit()


def metadata_location_by_name_step1(connection, name, connection2):
    location_ids = []
    count    = 0
    logging.info("[{0}] Adding location metadata to {1}".format(__name__, name))
    for row in good_readings_iterable(connection, name):
        location_id = get_location_id(connection, connection2, name, row[0], row[1], row[2])
        location_ids.append(location_id)
        if len(location_ids) == ROWS_PER_COMMIT:
            count += ROWS_PER_COMMIT
            update_location_id(connection, location_ids)
            location_ids = []
            logging.info("[{0}] Updated location metadata to {1} until {2}".format(__name__, name, row[0]))
    if len(location_ids):
        count += len(location_ids)
        update_location_id(connection, location_ids)
    logging.info("[{0}] Provisionally updated {1} locations for {2}.".format(__name__, count, name))


def metadata_location_by_name_step2(connection, name, connection2):

    global gap_list

    gap_list[name] = collections.deque()
    # First, detect the gaps
    logging.debug("[{0}] Detecting location gaps for {1}.".format(__name__, name))
    for items in shift_generator(good_readings_iterable2(connection, name), 2):
        if not all(items):
            continue
        old, new = items
        if old[2] != TEMP_REJECTED_LOCATION_ID and new[2] == TEMP_REJECTED_LOCATION_ID:
            gap = LocationGap(connection, name, connection2)
            gap.markStart(new[0], new[1], old[2])
            gap_list[name].append(gap)
        elif old[2] == TEMP_REJECTED_LOCATION_ID and new[2] != TEMP_REJECTED_LOCATION_ID:
            gap_list[name][-1].markEnd(old[0], old[1], new[2])

    # Finally, close the gaps
    logging.info("[{0}] Found {1} location gap candidates for {2}.".format(__name__, len(gap_list[name]), name))
    for gap in gap_list[name]:
        gap.fix()
    logging.info("[{0}] Location metadata update finally done for {1}".format(__name__, name))


def metadata_location_by_name(connection, name, connection2):
    metadata_location_by_name_step1(connection, name, connection2)
    # Close the detrected gaps
    metadata_location_by_name_step2(connection, name, connection2)
    


# ==============
# MAIN FUNCTIONS
# ==============

def metadata_location(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    if options.name is not None:
        metadata_location_by_name(connection, options.name, connection2)
    else:
        for name in candidate_names_iterable(connection):
            metadata_location_by_name(connection, name[0], connection2)

