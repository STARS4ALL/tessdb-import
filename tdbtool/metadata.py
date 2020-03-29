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
from .utils import open_database, open_reference_database, candidate_names_iterable, shift_generator

# ----------------
# Module constants
# ----------------

ROWS_PER_COMMIT = 50000

FLAGS_SUBSCRIBER_IMPORTED = 2


# -----------------------
# Module global variables
# -----------------------


# --------------
# Module classes
# --------------


# -----------------------
# Module global functions
# -----------------------

def metadata_create_index(connection):
    '''
    Create an index to speed up reqdings and location lookups
    '''
    logging.info("[{0}] Creating covering index on reference database".format(__name__))
    cursor = connection.cursor()
    cursor.execute(
        '''
        CREATE INDEX IF NOT EXISTS tess_readings_i2 
        ON tess_readings_t(tess_id, date_id, time_id, sequence_number, location_id)
        ''')
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



def metadata_refresh(connection, options):
    logging.info("[{0}] Opening reference database {1}".format(__name__, options.dbase))
    connection2 = open_reference_database(options.dbase)
    metadata_create_index(connection2)
    logging.info("[{0}] Done!".format(__name__))
   
