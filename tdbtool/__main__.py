# -*- coding: utf-8 -*-
# TESS DATABASE TOOL TO IMPORT MISSING DATA FROM OTHER DATABASES
# ----------------------------------------------------------------------
# Copyright (c) 2920 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

import sys
import argparse
import sqlite3
import os
import os.path
import datetime

#--------------
# other imports
# -------------

from . import __version__

from .input import *


# ----------------
# Module constants
# ----------------

DEFAULT_DBASE = "/var/dbase/tess.db"
EXTRA_DBASE   = "/var/dbase/extra.db"
DEFAULT_SQLITE_MODULE = "/usr/local/lib/libsqlitefunctions.so"

# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------

def utf8(s):
    if sys.version_info[0] < 3:
        return unicode(s, 'utf8')
    else:
        return (s)

def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    logging.info("[{0}] Opening database {1}".format(__name__, dbase_path))
    return sqlite3.connect(dbase_path)


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
    logging.info("[{0}] Creating data model from {1}".format(__name__, datamodel_path))
    conn1.executescript(script)

def createParser():
    # create the top-level parser
    name = os.path.split(os.path.dirname(sys.argv[0]))[-1]
    parser    = argparse.ArgumentParser(prog=name, description="tessdb import tools")

    # Global options
    parser.add_argument('--version', action='version', version='{0} {1}'.format(name, __version__))
    parser.add_argument('-d', '--dbase', default=DEFAULT_DBASE, help='SQLite database full file path')
    parser.add_argument('-x', '--extra-dbase', default=EXTRA_DBASE, help='SQLite extra database full file path')
    group1 = parser.add_mutually_exclusive_group()
    group1.add_argument('-v', '--verbose', action='store_true', help='Verbose output.')
    group1.add_argument('-q', '--quiet',   action='store_true', help='Quiet output.')

    # --------------------------
    # Create first level parsers
    # --------------------------

    subparser = parser.add_subparsers(dest='command')
    parser_input = subparser.add_parser('input', help='input commands')

    # ------------------------------------------
    # Create second level parsers for 'input'
    # ------------------------------------------
    # Choices:
    #   tdbtool input slurp
    #
    subparser = parser_input.add_subparsers(dest='subcommand')
    inp = subparser.add_parser('slurp', help='ingest input file')
    inp.add_argument('--csv-file', required=True, type=str, help='CSV file to ingest')

    return parser


def open_database(dbase_path):
    if not os.path.exists(dbase_path):
       raise IOError("No SQLite3 Database file found at {0}. Exiting ...".format(dbase_path))
    logging.info("[{0}] Opening database {1}".format(__name__, dbase_path))
    return sqlite3.connect(dbase_path)


def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)


def create_datamodel(connection, options):
    datamodel_path = resource_filename(__name__, 'sql/extra.sql')
    with open(datamodel_path) as f: 
        lines = f.readlines() 
    script = ''.join(lines)
    logging.info("[{0}] Creating data model from {1}".format(__name__,datamodel_path))
    connection.executescript(script)




def main():
    '''
    Utility entry point
    '''
    try:
        options = createParser().parse_args(sys.argv[1:])
        configureLogging(options)
        conn1 = open_database(options.extra_dbase)
        conn1.enable_load_extension(True)
        conn1.load_extension(DEFAULT_SQLITE_MODULE)
        conn2 = open_database(options.dbase)
        create_datamodel(conn1, options)
        command    = options.command
        subcommand = options.subcommand
        # Call the function dynamically
        func = command + '_' + subcommand
        globals()[func](conn1, options)
    except KeyboardInterrupt as e:
        logging.error("[{0}] Interrupted by user ".format(__name__))
    except Exception as e:
        logging.error("[{0}] Error => {1}".format(__name__, utf8(str(e)) ))
    finally:
        pass

main()



