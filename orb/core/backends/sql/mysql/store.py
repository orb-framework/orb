"""
Defines the data store logic for MySQL databases.
"""

from orb import DataStore


class MySQLDataStore(DataStore):
    pass

DataStore.registerAddon('MySQL', MySQLDataStore)
