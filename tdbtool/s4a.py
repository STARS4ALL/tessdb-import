# -*- coding: utf-8 -*-
# STARS4ALL UTILITY MODULE
# ----------------------------------------------------------------------
# Copyright (c) 2020 Rafael Gonzalez.
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
import datetime as Datetime


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

TSTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S"

# -----------------------
# Module global variables
# -----------------------

# -----------------------
# Module global functions
# -----------------------

def utf8(s):
    return  unicode(s, 'utf8') if sys.version_info[0] < 3 else s



# --------------
# Module classes
# --------------

class datetime(Datetime.datetime):
    '''
    This class adds convenience methods to the standard datetime class
    for all STARS4ALL related projects
    '''

    @classmethod
    def from_iso8601(cls, timestamp, fmt=TSTAMP_FORMAT):
        '''
        Creates a datetime from an ISO8601 string of the format YYYY-MM-DDTHH:MM:SS
        '''
        try:
            t = Datetime.datetime.strptime(timestamp, fmt)
        except Exception as e:
            return None
        return cls(t.year, t.month, t.day, t.hour, t.minute, t.second)

    @classmethod
    def from_dbase_ids(cls, date_id, time_id):
        yyyy = int(str(date_id)[0:2])
        mmmm = int(str(date_id)[2:4])
        dd   = int(str(date_id)[4:6])
        hh   = int(str(time_id)[0:2])
        mm   = int(str(time_id)[2:4])
        ss   = int(str(time_id)[4:6])
        return cls(yyyy, mmmm, dd, hh, mm, ss)


    def to_iso8601(self, fmt=TSTAMP_FORMAT):
        '''
        Produces an ISO 8601 string, YYYY-MM-DDTHH:MM:SS by defaault
        '''
        return self.strftime(fmt)


    def round(self):
        '''
        Round datetime to the nearest second
        '''
        return (self + Datetime.timedelta(microseconds=500000)).replace(microsecond=0)


    def to_dbase_ids(self):
        '''
        Return date and time database identifiers
        '''
        return 10000*self.year + 100*self.month + self.day, 10000*self.hour + 100*self.minute + self.second

