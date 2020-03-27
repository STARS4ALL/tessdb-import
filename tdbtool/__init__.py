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

# rejection codes
DUP_SEQ_NUMBER  = "Dup Sequence Number"
SINGLE          = "Single"
PAIR            = "Pair"
DAYLIGHT        = "Daylight"
BEFORE          = "Before registry"
AMBIGUOUS_LOC   = "Ambiguous Location Id"
COINCIDENT      = "Coincident reading in reference database"
SHIFTED         = "Timestamp shifted, same seq#"
AMBIGUOUS_TIME  = "Timestamp shifted"


# -----------------------
# Module global variables
# -----------------------

__version__ = get_versions()['version']



del get_versions
