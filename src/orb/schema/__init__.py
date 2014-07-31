#!/usr/bin/python

""" Defines the meta information for a column within a table schema. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = ['Eric Hulser']
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

from .aggregates import *
from .column import Column
from .index import Index
from .pipe import Pipe
from .table import Table
from .tablebase import TableBase
from .tableenum import TableEnum
from .tablegroup import TableGroup
from .tableschema import TableSchema
