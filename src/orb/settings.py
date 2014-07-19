#!/usr/bin/python

""" Defines common globals to use for the Orb system. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2012, Projex Software'
__license__         = 'LGPL'

# maintenance information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import os

# define timezone option (default will use tzlocal to determine local location)
# but you can specify on startup using the ORB_TIMEZONE environment key
TIMEZONE = ''
RAISE_BACKEND_ERRORS = False
CACHING_ENABLED = True
MAX_CACHE_TIMEOUT = 60 * 24     # maximum cache will be 1 day

# define primary defaults
PRIMARY_FIELD   = '_id'
PRIMARY_SETTER  = '_setId'
PRIMARY_GETTER  = '_id'
PRIMARY_DISPLAY = 'Id'
PRIMARY_INDEX   = 'byId'

# define inherits default
INHERIT_FIELD   = '__inherits__'

EDIT_ONLY_MODE  = False

# optimization flags
OPTIMIZE_DEFAULT_EMPTY = None

# initialize data from the environment
for key in globals().keys():
    val = os.environ.get('ORB_'+key)
    if val is not None:
        try:
            val = eval(val)
        except:
            pass
        
        globals()[key] = val