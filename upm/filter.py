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


import upm.s4a as s4a
from . import __version__

# ----------------
# Module constants
# ----------------

DEFAULT_DBASE = "/var/dbase/tess.db"
EXTRA_DBASE   = "/var/dbase/extra.db"
OUTPUT_FILE   = "~/output.sql"


TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


# Default dates whend adjusting in a rwnge of dates
DEFAULT_START_DATE = s4a.datetime(year=2000,month=1,day=1)
DEFAULT_END_DATE   = s4a.datetime(year=2999,month=12,day=31)

# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------

def utf8(s):
    return  unicode(s, 'utf8') if sys.version_info[0] < 3 else s
 

def mkdate(datestr):
    try:
        date = upm.s4a.datetime.strptime(datestr, '%Y-%m-%d').replace(hour=12)
    except ValueError:
        date = s4a.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    return date


def now_month():
    return s4a.datetime.utcnow().replace(day=1,hour=0,minute=0,second=0,microsecond=0)

def mkmonth(datestr):
    return s4a.datetime.strptime(datestr, MONTH_FORMAT)

def result_generator(cursor, arraysize=500):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result


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



def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    logging.info("Opening database {0}".format(dbase_path))
    return sqlite3.connect(dbase_path)


def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description="tessdb command line tool")
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
    parser.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    parser.add_argument('-x', '--extra-dbase', default=EXTRA_DBASE, help='SQLite extra database full file path')
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
    group1.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')
    return parser


def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)

def create_datamodel(options, conn1):
    datamodel_path = resource_filename(__name__, 'sql/extra.sql')
    with open(datamodel_path) as f: 
        lines = f.readlines() 
    script = ''.join(lines)
    logging.info("Creating data model from {0}".format(datamodel_path))
    conn1.executescript(script)

def slurp(conn, iterable):
    cursor = conn.cursor()
    try:
        cursor.executemany(
        '''
        INSERT OR IGNORE INTO raw_readings_t(id, date_id, time_id, tess, sequence_number, frequency, magnitude, ambient_temperature, sky_temperature, signal_strength) 
        VALUES (?, ?,?,?,?,?,?,?,?,?)
        ''', iterable)
    except Exception as e:
        print(e)
    conn.commit()



def main():
    '''
    Utility entry point
    '''
    try:
        options = createParser().parse_args(sys.argv[1:])
        configureLogging(options)
        conn1 = open_database(options.extra_dbase)
        conn1.enable_load_extension(True)
        conn1.load_extension("/usr/local/lib/libsqlitefunctions.so")
        conn2 = open_database(options.dbase)
        create_datamodel(options, conn1)
        slurp(conn1, csv_generator("/home/rafa/repos/tessdb-import/pruebas/dump_mongodb.csv"))



    except KeyboardInterrupt:
        print('')
    #except Exception as e:
        print("Error => {0}".format( utf8(str(e)) ))
    finally:
        pass
# ==============
# DATABASE STUFF
# ==============


# ----------------------
# INSTRUMENT SUBCOMMANDS
# ----------------------

def instrument_assign(connection, options):
    cursor = connection.cursor()
    row = {'site': options.location,  'state': CURRENT}
    cursor.execute("SELECT location_id FROM location_t WHERE site == :site",row)
    res =  cursor.fetchone()
    if not res:
        print("Location not found by {0}".format(row['site']))
        sys.exit(1)
    row['loc_id'] = res[0]
    if options.name is not None:
        row['name'] = options.name
        cursor.execute(
            '''
            UPDATE tess_t SET location_id = :loc_id
            WHERE mac_address IN (SELECT mac_address FROM name_to_mac_t WHERE name == :name AND valid_state == :state)
            ''', row)
    else:
         row['mac'] = options.mac
         cursor.execute(
            '''
            UPDATE tess_t SET location_id = :loc_id
            WHERE mac_address = :mac)
            ''', row)
    
    cursor.execute(
        '''
        SELECT name,mac_address,site
        FROM tess_v
        WHERE valid_state == :state
        AND name = :name
        ''',row)
    paging(cursor,["TESS","MAC","Site"])
    connection.commit()    



