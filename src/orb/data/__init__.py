#!/usr/bin/python

""" 
Defines methods for aggregation within the database system.
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

from .converter import DataConverter
from .join import Join
from .piperecordset import PipeRecordSet
from .recordset import RecordSet
from .store import DataStore