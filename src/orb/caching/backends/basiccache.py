import datetime

from projex.lazymodule import lazy_import
from orb.caching.datacache import DataCache
from projex.locks import ReadLocker, ReadWriteLock, WriteLocker

orb = lazy_import('orb')


class BasicCache(DataCache):
    """ Base caching object for tracking data caches """
    def __init__(self, timeout=0):
        super(BasicCache, self).__init__(timeout)

        # define custom properties
        self._cacheLock = ReadWriteLock()
        self._cache = {}
        self._cachedAt = {}

    def cachedAt(self, key):
        """
        Returns when the inputed key was last cached for this instance.

        :param      key | <hashable>
        """
        with ReadLocker(self._cacheLock):
            return self._cachedAt.get(key)

    def clear(self):
        """
        Clears out all the caching information for this instance.
        """
        with WriteLocker(self._cacheLock):
            self._cachedAt.clear()
            self._cache.clear()

    def expire(self, key):
        with WriteLocker(self._cacheLock):
            if key:
                self._cache.pop(key, None)
                self._cachedAt.pop(key, None)
            else:
                self._cache.clear()
                self._cachedAt.clear()

    def isCached(self, key):
        """
        Returns whether or not the inputed key is cached.

        :param      key | <hashable>

        :return     <bool>
        """
        with ReadLocker(self._cacheLock):
            return key in self._cache

    def isExpired(self, key):
        """
        Returns whether or not the current cache is expired.

        :return     <bool>
        """
        if not self.isEnabled():
            return True

        cachedAt = self.cachedAt(key)
        if cachedAt is None:
            return True

        # check to see if the cache is expired against the global expire time
        if orb.system.isCacheExpired(cachedAt):
            return True

        # grab the maximum number of seconds to store a cache within the
        # system (it is stored as minutes)
        max_timeout = orb.system.maxCacheTimeout() * 60
        if self.timeout() is None:
            timeout = max_timeout
        else:
            timeout = min(self.timeout(), max_timeout)

        now  = datetime.datetime.now()
        secs = int((now - cachedAt).total_seconds())

        return timeout < secs

    def setValue(self, key, value):
        """
        Caches the inputed key and value to this instance.

        :param      key     | <hashable>
                    value   | <variant>
        """
        with WriteLocker(self._cacheLock):
            self._cache[key] = value
            self._cachedAt[key] = datetime.datetime.now()

    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.

        :return     <variant>
        """
        with ReadLocker(self._cacheLock):
            return self._cache.get(key)

# create a basic cache object
DataCache.registerAddon('Basic', BasicCache)