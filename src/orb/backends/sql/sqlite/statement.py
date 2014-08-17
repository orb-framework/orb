"""
Defines the base SQLiteStatement class used for all SQLite based statments.
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

from ..abstractstatement import AbstractSQLStatement

class SQLiteStatement(AbstractSQLStatement):
    pass


# register the SQLite addon interface to the base SQLStatement class
AbstractSQLStatement.registerAddon('SQLite', SQLiteStatement)

# register the statements addons for SQLite
from .statements import __plugins__
SQLiteStatement.registerAddonModule(__plugins__)