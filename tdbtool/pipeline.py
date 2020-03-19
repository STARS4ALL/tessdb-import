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
import traceback

# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__
from .      import DUP_SEQ_NUMBER, SINGLE, PAIR, TSTAMP_FORMAT
from .utils import paging, previous_iterable

from .input    import input_slurp, input_differences, input_retained
from .stats    import stats_daily, stats_global
from .daylight import daylight_detect
from .metadata import metadata_flags, metadata_location, metadata_instrument

# ----------------
# Module constants
# ----------------

# -----------------------
# Module global variables
# -----------------------



# ==============
# MAIN FUNCTIONS
# ==============

def pipeline_stage1(connection, options):
    logging.info("[{0}] =============== PIPELINE STAGE 1 STEP 1 ===============".format(__name__))
    input_slurp(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 1 STEP 2 ===============".format(__name__))
    input_differences(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 1 STEP 3 ===============".format(__name__))
    stats_daily(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 1 STEP 4 ===============".format(__name__))
    stats_global(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 1 STEP 5 ===============".format(__name__))
    input_retained(connection, options)

def pipeline_stage2(connection, options):
    logging.info("[{0}] =============== PIPELINE STAGE 2 STEP 1 ===============".format(__name__))
    daylight_detect(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 2 STEP 2 ===============".format(__name__))
    metadata_instrument(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 2 STEP 3 ===============".format(__name__))
    metadata_location(connection, options)
    logging.info("[{0}] =============== PIPELINE STAGE 2 STEP 4 ===============".format(__name__))
    metadata_flags(connection, options)

def pipeline_full(connection, options):
    pipeline_stage1(connection, options)
    pipeline_stage2(connection, options)

    