#!/usr/bin/python

"""
Defines the base connection class that will be used for communication
to the backend databases.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

from .connection import Connection
from .database import Database
from .environment import Environment
from .options import LookupOptions, DatabaseOptions
from .transaction import Transaction