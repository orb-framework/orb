#!/usr/bin/python

""" 
Defines the querying classes and abstractions for ORB
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

from .query import Query
from .querycompound import QueryCompound
from .queryaggregate import QueryAggregate
from .querypattern import QueryPattern