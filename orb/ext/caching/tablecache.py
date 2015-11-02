"""
Defines caching methods to use when working with tables.
"""

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class TableCache(object):
    """" Base class for referencing orb Table cache information """
    
    def __init__(self, table, cache, timeout=None):
        self._cache = cache
        self._table = table
        self._timeout = timeout
        self._preloaded = table.schema().preloadCache()

    def __getitem__(self, key):
        return self._cache[self.__key(key)]

    def __setitem__(self, key, value):
        self._cache[self.__key(key)] = value

    def __key(self, key):
        """
        Create the key for this table cache by joining the table name with the inputted key.

        :param      key | <str>
        """
        return '{0}({1})'.format(self.table().schema().name(), key)

    def cache(self):
        """
        Returns the cache object associated with this table.

        :return     <orb.caching.DataCache> || None
        """
        return self._cache

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
        self._table = table

    def setTimeout(self, seconds):
        """
        Sets the timeout length for this table cache to the inputted amount.

        :param      seconds | <int> || None
        """
        self._timeout = seconds

    def setValue(self, key, value, timeout=None):
        """
        Stores the given key as the given value.

        :param      key     | <hashable>
                    value   | <variant>
        """
        self._cache.setValue(self.__key(key), value, timeout=timeout or self.timeout())

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

        :return     <int> | seconds
        """
        return self._timeout