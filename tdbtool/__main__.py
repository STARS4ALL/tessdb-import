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
import logging
import traceback

# Access  template withing the package
from pkg_resources import resource_filename

#--------------
# other imports
# -------------

from . import __version__

from .utils import utf8, mkdate
from .input import input_slurp, input_differences
from .stats import stats_daily, stats_global, stats_retained, stats_inspect
from .plot  import plot_histogram


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
    parser_stats = subparser.add_parser('stats', help='plot commands')
    parser_plot  = subparser.add_parser('plot',  help='plot commands')
   

    # ------------------------------------------
    # Create second level parsers for 'input'
    # ------------------------------------------
  
    subparser = parser_input.add_subparsers(dest='subcommand')
    isl = subparser.add_parser('slurp', help='ingest input file')
    isl.add_argument('--csv-file', required=True, type=str, help='CSV file to ingest')
    ist = subparser.add_parser('differences', help='compute differences between consecutive readings')

    # ------------------------------------------
    # Create second level parsers for 'stats'
    # ------------------------------------------
  
    subparser = parser_stats.add_subparsers(dest='subcommand')
    sdy = subparser.add_parser('daily',  help='compute daily period statistics')
    
    sgl = subparser.add_parser('global', help='compute global period statistics')
    sgl.add_argument('--name', type=str, help='TESS-W name to set the global period to')
    sgl.add_argument('--period', type=float, metavar='<T>', help='Set global period for a given TESS-W')

    sre = subparser.add_parser('retained', help='figure out retained values')
    sre.add_argument('--name', required=True, type=str, help='TESS-W name to set the global period to')
    sre.add_argument('--period', type=float, metavar='<T>', help='period for a given TESS-W')
    sre.add_argument('--tolerance', type=int, default= 0, metavar='<%>', help='period tolerance to add')
    sre.add_argument('--display', action='store_true', help='display candidates only')

    sin = subparser.add_parser('inspect', help='Inspect input values around a given rank')
    sin.add_argument('--name', required=True, type=str, help='TESS-W name to set the global period to')
    sin.add_argument('--rank', required=True, type=int, metavar='<N>', help='rank order')
    sin.add_argument('--width', type=int, default= 3, metavar='<N>', help='display width')

   
    
    # ------------------------------------------
    # Create second level parsers for 'plot'
    # ------------------------------------------

    subparser = parser_plot.add_subparsers(dest='subcommand')
    phi = subparser.add_parser('histogram', help='TESS-W Tx period histogram from timestamps')
    phi.add_argument('--name', required=True, type=str, help='TESS-W name')
    phi.add_argument('--start-date', type=mkdate, metavar="<YYYY-MM-DD>", help='Optional start date')
    phi.add_argument('--end-date',   type=mkdate, metavar="<YYYY-MM-DD>", help='Optional end date')

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
        logging.error("[{0}] Fatal error => {1}".format(__name__, utf8(str(e)) ))
        traceback.print_exc()
    finally:
        pass

main()



