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
from .utils import open_database

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

FLAGS_SUBSCRIBER_IMPORTED = 2

# -----------------------
# Module global variables
# -----------------------


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
    logging.info("[{0}] Opening database {1}".format(__name__,options.dbase))
    connection2 = open_database(options.dbase)
    logging.info("[{0}] Done!".format(__name__))
 
def metadata_instrument(connection, options):
    logging.info("[{0}] adding instrument metadata".format(__name__))
    logging.info("[{0}] Opening database {1}".format(__name__, options.dbase))
    connection2 = open_database(options.dbase)
    logging.info("[{0}] Done!".format(__name__))

