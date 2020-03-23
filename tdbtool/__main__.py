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

from .utils import utf8, mkdate, percent, open_database
from .input import input_slurp, input_differences, input_retained
from .stats import stats_daily, stats_global
from .show  import show_global, show_daily, show_differences, show_duplicated, show_count
from .plot  import plot_period, plot_differences
from .pipeline import pipeline_stage1, pipeline_stage2, pipeline_full
from .daylight import daylight_detect
from .metadata import metadata_flags, metadata_refresh
from .location import metadata_location
from .instrument import metadata_instrument

# ----------------
# Module constants
# ----------------

DEFAULT_DBASE = "/var/dbase/tess.db"
EXTRA_DBASE   = "/var/dbase/extra.db"
SQLITE_MATH_MODULE   = "/usr/local/lib/libsqlitefunctions.so"
SQLITE_REGEXP_MODULE = "/usr/lib/sqlite3/pcre.so"

# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------



def configureLogging(options):
    if options.verbose:
        level = logging.DEBUG
    elif options.quiet:
        level = logging.WARN
    else:
        level = logging.INFO
    logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=level)


def create_datamodel(options, connection):
    datamodel_path = resource_filename(__name__, 'sql/extra.sql')
    with open(datamodel_path) as f: 
        lines = f.readlines() 
    script = ''.join(lines)
    logging.info("[{0}] Creating data model from {1}".format(__name__, datamodel_path))
    connection.executescript(script)


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
    parser_show  = subparser.add_parser('show',  help='show commands')
    parser_day   = subparser.add_parser('daylight', help='daylight commands')
    parser_pipe  = subparser.add_parser('pipeline', help='pipeline commands')
    parser_meta  = subparser.add_parser('metadata', help='metadata commands')

    # ------------------------------------------
    # Create second level parsers for 'input'
    # ------------------------------------------
  
    subparser = parser_input.add_subparsers(dest='subcommand')
    isl = subparser.add_parser('slurp', help='ingest input file')
    isl.add_argument('--csv-file', required=True, type=str, help='CSV file to ingest')
    isl.add_argument('--name', type=str, help='Optional TESS-W name to filter')

    ist = subparser.add_parser('differences', help='compute differences between consecutive readings')
    ist.add_argument('--name', type=str, help='Optional TESS-W name to filter')

    isr = subparser.add_parser('retained', help='fix isolated out retained values')
    isr.add_argument('--name', type=str, help='Optional TESS-W name')
    isr.add_argument('--test', action='store_true', help='Test only, do not update candidates')
    isr.add_argument('--limit', type=int, default=10, metavar="<N>", help='Optional limit to display in test mode')

    # ------------------------------------------
    # Create second level parsers for 'stats'
    # ------------------------------------------
  
    subparser = parser_stats.add_subparsers(dest='subcommand')
    sdy = subparser.add_parser('daily',  help='compute daily period statistics')
    sdy.add_argument('--name', type=str, help='Optional TESS-W name')
    
    sgl = subparser.add_parser('global', help='compute global period statistics')
    sgl.add_argument('--name', type=str, help='Optional TESS-W name')
    sgl.add_argument('--period', type=float, metavar='<T>', help='Set global period for a given TESS-W')
    
    # ------------------------------------------
    # Create second level parsers for 'plot'
    # ------------------------------------------

    subparser = parser_plot.add_subparsers(dest='subcommand')
    ppe = subparser.add_parser('period', help='TESS-W Tx period 1-D histogram')
    ppe.add_argument('--name', required=True, type=str, help='TESS-W name')
    ppe.add_argument('--start-date', type=mkdate, metavar="<YYYY-MM-DD>", help='Optional start date')
    ppe.add_argument('--end-date',   type=mkdate, metavar="<YYYY-MM-DD>", help='Optional end date')
    ppe.add_argument('--bins',  type=int, default=50, metavar="<N>", help='Number of histogram bins')
    ppe.add_argument('--central',   choices=["mean", "median"], default="median", help='Central tendency estimate')

    pdi = subparser.add_parser('differences', help='TESS-W Tx period 2-D histogram differences')
    pdi.add_argument('--name', required=True, type=str, help='TESS-W name')
    pdi.add_argument('--start-date', type=mkdate, metavar="<YYYY-MM-DD>", help='Optional start date')
    pdi.add_argument('--end-date',   type=mkdate, metavar="<YYYY-MM-DD>", help='Optional end date')
    pdi.add_argument('--bins',  type=int, default=20, metavar="<N>", help='Number of histogram bins')

    # ------------------------------------------
    # Create second level parsers for 'daylight'
    # ------------------------------------------

    subparser = parser_day.add_subparsers(dest='subcommand')
    pdd = subparser.add_parser('detect', help='Detect daylight readings')
    pdd.add_argument('--name', type=str, help='Optional TESS-W name')


    # ------------------------------------------
    # Create second level parsers for 'pipeline'
    # ------------------------------------------

    subparser = parser_pipe.add_subparsers(dest='subcommand')
    pp1 = subparser.add_parser('stage1', help='Stage 1 Pipeline')
    pp1.add_argument('--csv-file', required=True, type=str, help='CSV file to ingest')
    pp1.add_argument('--name', type=str, help='Optional TESS-W name')
    
    pp2 = subparser.add_parser('stage2', help='Stage 2 Pipeline')
    pp2.add_argument('--name', type=str, help='Optional TESS-W name')
   
    ppf = subparser.add_parser('full', help='Full Pipeline')
    ppf.add_argument('--csv-file', required=True, type=str, help='CSV file to ingest')
    ppf.add_argument('--name', type=str, help='Optional TESS-W name')

    # ------------------------------------------
    # Create second level parsers for 'metadata'
    # ------------------------------------------

    subparser = parser_meta.add_subparsers(dest='subcommand')
    
    pmr = subparser.add_parser('refresh', help='Refresh metadata aggregates from reference database')
    pmr.add_argument('--name', type=str, help='Optional TESS-W name')
    
    pmf = subparser.add_parser('flags', help='Add flags metadata to readings')
    pmf.add_argument('--name', type=str, help='Optional TESS-W name')

    pml = subparser.add_parser('location', help='Add location metadata to readings')
    pml.add_argument('--name', type=str, help='Optional TESS-W name')

    pmi = subparser.add_parser('instrument', help='Add instrument metadata to readings')
    pmi.add_argument('--name', type=str, help='Optional TESS-W name')



    # ------------------------------------------
    # Create second level parsers for 'show'
    # ------------------------------------------

    subparser = parser_show.add_subparsers(dest='subcommand')
    shu = subparser.add_parser('duplicated', help='Show duplicated sequence numbers')
    shu.add_argument('--name', required=True, type=str, help='TESS-W name')
    shu.add_argument('--start-date', type=mkdate, metavar="<YYYY-MM-DD>", help='Optional start date')
    shu.add_argument('--end-date',   type=mkdate, metavar="<YYYY-MM-DD>", help='Optional end date')
    shu.add_argument('--limit',      type=int, default=10, metavar="<N>", help='Optional limit')

    shg = subparser.add_parser('global', help='show global period statistics')
    shg.add_argument('--name', type=str, help='optional TESS-W name')
    shg.add_argument('--limit',  type=int, default=10, metavar="<N>", help='Optional limit')

    shd = subparser.add_parser('daily', help='show daily period statistics')
    shd.add_argument('--name', type=str, help='optional TESS-W')
    shd.add_argument('--limit',  type=int, default=10, metavar="<N>", help='Optional limit')

    shi = subparser.add_parser('differences', help='show entries in the differences table')
    shi.add_argument('--name', type=str, help='optional TESS-W')
    shi.add_argument('--limit',  type=int, default=10, metavar="<N>", help='Optional limit')

    sha = subparser.add_parser('around', help='Show input values around a given rank')
    sha.add_argument('--name', required=True, type=str, help='TESS-W name to set the global period to')
    sha.add_argument('--rank', required=True, type=int, metavar='<N>', help='rank order')
    sha.add_argument('--width', type=int, default= 3, metavar='<N>', help='display width')

    shc = subparser.add_parser('count', help='show counts')
    shc.add_argument('--name', type=str, help='Optional TESS-W name')
    shcex = shc.add_mutually_exclusive_group(required=True)
    shcex.add_argument('--valid', action="store_true", help='Number of valid readings')
    shcex.add_argument('--duplicated', action="store_true", help='Number of duplicated sequence numbers')
    shcex.add_argument('--single', action="store_true", help='Number of single (per day) readings')
    shcex.add_argument('--pairs',  action="store_true", help='Number of pairs (per day) readings')
    shcex.add_argument('--daylight',  action="store_true", help='Number daylight readings')

    return parser


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
        logging.info("[{0}] Opening database {1}".format(__name__,options.extra_dbase))
        connection = open_database(options.extra_dbase)
        connection.enable_load_extension(True)
        connection.load_extension(SQLITE_MATH_MODULE)
        connection.load_extension(SQLITE_REGEXP_MODULE)
        create_datamodel(connection, options)
        command    = options.command
        subcommand = options.subcommand
        # Call the function dynamically
        func = command + '_' + subcommand
        globals()[func](connection, options)
    except KeyboardInterrupt as e:
        logging.error("[{0}] Interrupted by user ".format(__name__))
    except Exception as e:
        logging.error("[{0}] Fatal error => {1}".format(__name__, utf8(str(e)) ))
        traceback.print_exc()
    finally:
        pass

main()



