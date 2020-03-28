# ----------------------------------------------------------------------
# Copyright (c) 2014 Rafael Gonzalez.
#
# See the LICENSE file for details
# ----------------------------------------------------------------------

#--------------------
# System wide imports
# -------------------

# ---------------
# Twisted imports
# ---------------

#--------------
# local imports
# -------------

from ._version import get_versions

# ----------------
# Module constants
# ----------------

TSTAMP_FORMAT   = "%Y-%m-%dT%H:%M:%SZ"

# **** rejection codes ****

PROV_ACCEPTED   = -1

# Reading finally accepted and ready to be inserted into the reference database
ACCEPTED        = 0

# Foudn two readings with the same seqnece number (retained data?)
DUP_SEQ_NUMBER  = 1 # "Dup Sequence Number"

# Found a single reading within a day
SINGLE          = 2 # "Single"

# Found two readings within a day
PAIR            = 3 # "Pair"

# Reading in daylight
DAYLIGHT        = 4 # "Daylight"

# Reading timestamp is before photoemetr registration in reference database
BEFORE          = 5 # "Before registry"

# Reading timestamp between a change between two different location ids. Could not choose.
AMBIGUOUS_LOC   = 6 # "Ambiguous Location Id"

# This reading - within the timemp tolerance - already exists in the reference database
COINCIDENT      = 7 # "Coincident reading in reference database"

# Found another reading within the timestamp tolerance and another sequence number.
# This is a sign of bad NTP synchronization
SHIFTED         = 8 # "Timestamp shifted, same seq#"

# Found two readings within the timestamp tolerance
# A server form of lack of NTP synchronization
AMBIGUOUS_TIME  = 9 # "More than 1 referemce sample found"


# -----------------------
# Module global variables
# -----------------------

__version__ = get_versions()['version']



del get_versions
