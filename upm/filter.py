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
import os
import os.path
import datetime
import logging

# Access  template withing the package
from pkg_resources import resource_filename


# Python3 catch
try:
    raw_input
except:
    raw_input = input 

# ----------------
# Module constants
# ----------------

DEFAULT_DBASE = "~/tess.db"
EXTRA_DBASE   = "~/extra.db"
OUTPUT_FILE   = "~/output.sql"


TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"


# Default dates whend adjusting in a rwnge of dates
DEFAULT_START_DATE = datetime.datetime(year=2000,month=1,day=1)
DEFAULT_END_DATE   = datetime.datetime(year=2999,month=12,day=31)

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
        date = datetime.datetime.strptime(datestr, '%Y-%m-%d').replace(hour=12)
    except ValueError:
        date = datetime.datetime.strptime(datestr, '%Y-%m-%dT%H:%M:%S')
    return date


def now_month():
    return datetime.datetime.utcnow().replace(day=1,hour=0,minute=0,second=0,microsecond=0)

def mkmonth(datestr):
    return datetime.datetime.strptime(datestr, MONTH_FORMAT)

def result_generator(cursor, arraysize=500):
    'An iterator that uses fetchmany to keep memory usage down'
    while True:
        results = cursor.fetchmany(arraysize)
        if not results:
            break
        for result in results:
            yield result

def createMonthList(options):
    if options.latest_month:
        start_month  = now_month()
        end_month   = start_month
    elif options.previous_month:
        start_month  = now_month() + relativedelta(months = -1)
        end_month    = start_month
    elif options.for_month:
        start_month = options.for_month
        end_month   = start_month
    else:
        start_month  = options.from_month
        end_month    = now_month()
    return MonthIterator(start_month, end_month)

def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)

def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    logging.info("Opening database {0}".format(dbase_path))
    return sqlite3.connect(dbase_path)


def render(template_path, context):
    if not os.path.exists(template_path):
        raise IOError("No Jinja2 template file found at {0}. Exiting ...".format(template_path))
    path, filename = os.path.split(template_path)
    return jinja2.Environment(
        loader=jinja2.FileSystemLoader(path or './')
    ).get_template(filename).render(context)

def pep():
    template_path = resource_filename(__name__, 'templates/SQL-template.j2')

def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description="tessdb command line tool")
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
    parser.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    parser.add_argument('-x', '--extra-database',  default=DEFAULT_DBASE, help='SQLite database full file path')
    parser.add_argument('-o', '-output-file',  default=DEFAULT_FILE, help='Default output file')
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose',  action='store_true', help='verbose output')
    group1.add_argument('-q', '--quiet',    action='store_true', help='quiet output')
    return parser

def main():
    '''
    Utility entry point
    '''
    try:
        options = createParser().parse_args(sys.argv[1:])
        configureLogging(options)
        conn1 = open_database(options.extra_dbase)
        conn2 = open_database(options.dbase)
        
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



