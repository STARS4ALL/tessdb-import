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

import sys
import os
import os.path
import argparse
import sqlite3
import logging
import traceback


# ----------------
# MatPlotLib stuff
# ----------------

import matplotlib.pyplot as plt
import numpy             as np

from matplotlib        import colors
from matplotlib.ticker import PercentFormatter


# -------------
# Local imports
# -------------

import tdbtool.s4a
from .      import __version__

# ----------------
# Module constants
# ----------------

TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

# -----------------------
# Module global variables
# -----------------------



# --------------
# Module classes
# --------------


# -----------------------
# Module global functions
# -----------------------



# ==============
# MAIN FUNCTIONS
# ==============


def plot_histogram(connection, options):
    plt.ioff()  # Turns off interactive mode
    pass


