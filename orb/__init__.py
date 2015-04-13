"""
ORB stands for Object Relation Builder and is a powerful yet simple to use \
database class generator.
"""

# define authorship information
__authors__ = ['Eric Hulser']
__author__ = ','.join(__authors__)
__credits__ = []
__copyright__ = 'Copyright (c) 2011, Projex Software'
__license__ = 'LGPL'

# maintenance information
__maintainer__ = 'Eric Hulser'
__email__ = 'eric.hulser@gmail.com'

# ------------------------------------------------------------------------------

from .version import *

#------------------------------------------------------------------------------

import logging

logger = logging.getLogger(__name__)

# import global symbols
from .common import *
from .caching import *
from .core import *
from .data import *
from .querying import *
from .schema import *
from .searching import *
from .contexts import *

#----------------------------------------------------------------------

# create the global manager instance
from .manager import Manager

system = Manager.instance()

#----------------------------------------------------------------------
# backwards compatibility support (pre: 4.0.0)

OrbGroup = TableGroup
OrbThesaurus = SearchThesaurus
Orb = Manager

