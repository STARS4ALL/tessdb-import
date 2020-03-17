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
DUP_SEQ_NUMBER  = "Dup Sequence Number"
SINGLE          = "Single"
PAIR            = "Pair"
DAYLIGHT        = "Daylight"

# -----------------------
# Module global variables
# -----------------------

__version__ = get_versions()['version']



del get_versions
