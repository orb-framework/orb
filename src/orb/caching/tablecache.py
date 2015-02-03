#!/usr/bin/python

""" 
Defines caching methods to use when working with tables.
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

from projex.lazymodule import LazyModule
from .datacache import DataCache

orb = LazyModule('orb')


class TableCache(object):
    """" Base class for referencing orb Table cache information """
    
    def __init__(self, table=None, timeout=0):
        if timeout is not None:
            # determine the expire information
            table_expires = table.schema().cacheExpireIn()
            max_expires = orb.system.maxCacheTimeout()
            opts = [timeout, table_expires, max_expires]
            timeout = min(opts) * 60

        self._cache = DataCache.create(timeout)
        self._table = table
        if table:
            self._preloaded = table.schema().preloadCache()

    def __getitem__(self, key):
        return self._cache[self.__key(key)]

    def __setitem__(self, key, value):
        self._cache[self.__key(key)] = value

    def __key(self, key):
        """
        Create the key for this table cache by joining the table name with the inputed key.

        :param      key | <str>
        """
        return 'TableCache', self.table().schema().name() if self.table() else 'Table', key

    def cachedAt(self, key):
        """
        Returns the time when the given key was cached.

        :param      key | <str>
        """
        return self._cache.cachedAt(self.__key(key))

    def expire(self, key=None):
        """
        Expires the given key, or the whole cache if no key is provided.

        :param      key | <hashable> || None
        """
        return self._cache.expire(self.__key(key) if key else None)

    def isCached(self, key):
        """
        Returns whether or not the inputted key is cached.

        :param      key | <hashable> || None
        """
        return self._cache.isCached(self.__key(key))

    def isEnabled(self):
        """
        Returns whether or not this table's cache is enabled.

        :return     <bool>
        """
        return self._cache.isEnabled()

    def isExpired(self, key):
        """
        Returns whether or not the given key is expired for this
        caching instance.  This will also take into account any table
        record changes that will affect caching.
        
        :sa     Table.markTableCacheExpired
        
        :param      key | <hashable>
        """
        return self._cache.isExpired(self.__key(key))

    def isPreloaded(self):
        """
        Returns whether or not the cache is preloaded.
        
        :return     <bool>
        """
        return self._preloaded

    def setEnabled(self, state):
        """
        Sets whether or not to use the cache for this table.

        :param      state | <bool>
        """
        self._cache.setEnabled(state)

    def setPreloaded(self, state):
        """
        Sets whether or not the cache should be preloaded.
        
        :param      state | <bool>
        """
        self._preloaded = state

    def setTable(self, table):
        """
        Assign the table for this cache.

        :param      table | <orb.Table> || None
        """
        timeout = table.schema().cacheExpireIn() * 60 if table else 0
        self._cache.setTimeout(max(self._cache.timeout(), timeout))
        self._table = table

    def setTimeout(self, timeout):
        """
        Sets the timeout length for this table cache to the inputted amount.

        :param      timeout | <int> || None
        """
        self._cache.setTimeout(timeout)

    def setValue(self, key, value):
        """
        Stores the given key as the given value.

        :param      key     | <hashable>
                    value   | <variant>
        """
        self._cache.setValue(self.__key(key), value)

    def table(self):
        """
        Returns the table associated with this cache instance.
        
        :return     <orb.Table>
        """
        return self._table

    def value(self, key, default=None):
        """
        Returns the value for this cache's key.

        :param      key | <hashable>
                    default | <variant>
        """
        return self._cache.value(self.__key(key), default)

    def timeout(self):
        """
        Returns the timeout for this cache.

        :return     <int> | milliseconds
        """
        return self._cache.timeout()