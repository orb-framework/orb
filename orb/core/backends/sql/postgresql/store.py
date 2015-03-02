"""
Defines the data store logic for PSQL databases.
"""

from orb import DataStore


class PSQLDataStore(DataStore):
    pass

DataStore.registerAddon('Postgres', PSQLDataStore)
