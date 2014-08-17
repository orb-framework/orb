#!/usr/bin/python
""" 
Defines a searching algorithm for searching across multiple tables.
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

from .engine import SearchEngine
from .terms import SearchTerm, \
                   SearchTermGroup
from .spelling import SpellingEngine
from .thesaurus import SearchThesaurus