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

# Python3 catch
try:
    raw_input
except:
    raw_input = input 

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

def daily_period_iterable(connection, name, central):
	row = {'name': name}
	cursor = connection.cursor()
	if central == "median":
		cursor.execute(
			'''
			SELECT median_period
			FROM   daily_stats_t
			WHERE  name = :name
			''', row)
	else:
		cursor.execute(
			'''
			SELECT mean_period
			FROM   daily_stats_t
			WHERE  name = :name
			''', row)
	return cursor   # return Cursor as an iterable


def plot_period(connection, options):
	iterable = daily_period_iterable(connection, options.name, options.central)
	period = zip(*iterable)
	period = np.array(period[0], dtype=float)
	fig, axs = plt.subplots(1, 1, tight_layout=True)
	plt.ion()
	plt.yscale('log')
	plt.ylabel('Counts')
	#plt.grid(True)
	plt.grid(b=True, which='major', color='b', linestyle='-')
	plt.grid(b=True, which='minor', color='r', linestyle=':')
	plt.title('Period histogram for {0}'.format(options.name))
	axs.hist(period, bins=options.bins)
	plt.show()
	raw_input("Press <ENTER> to exit")



def plot_differences(connection, options):
	fig, axs = plt.subplots(1, 2, sharey=True, tight_layout=True)
	plt.yscale('log')
	plt.ylabel('Counts')
	plt.grid(True)
	plt.title('Histogram of Differnces in time and sequence numbers')
	min_seq, max_seq, min_sec, max_sec = daily_differences_range(connection, name)
	iterable = daily_differences_iterable(connection,name)
	time, seq_diff, seconds_diff = zip(*iterable)
	seconds_diff = np.array(seconds_diff, dtype=int)
	seq_diff     = np.array(seq_diff,     dtype=int)
	print("ARRAYS CREADOS")
	axs[0].hist(seconds_diff, bins=100)
	axs[1].hist(seq_diff,     bins=100)
	plt.show()



# ==============
# MAIN FUNCTIONS
# ==============


def plot_histogram(connection, options):
	plt.ioff()  # Turns off interactive mode
	pass


