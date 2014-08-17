"""
Defines the base MySQL class used for all Postgres based statments.
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

from ..abstractsql import SQL

class MySQL(SQL):
    pass

# register the MySQL addon interface to the base SQLStatement class
SQL.registerAddon('MySQL', MySQL)

# register the statements addons for MySQL
from .statements import __plugins__
MySQL.registerAddonModule(__plugins__)