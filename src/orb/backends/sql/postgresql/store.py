"""
Defines the data store logic for PSQL databases.
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

from orb import DataStore


class PSQLDataStore(DataStore):
    pass

DataStore.registerAddon('Postgres', PSQLDataStore)
