import datetime

from projex.lazymodule import lazy_import
from orb.caching.datacache import DataCache
from projex.locks import ReadLocker, ReadWriteLock, WriteLocker

orb = lazy_import('orb')


# noinspection PyAbstractClass
class BasicCache(DataCache):
    """ Base caching object for tracking data caches """

    def __init__(self, timeout=0):
        super(BasicCache, self).__init__(timeout)

        # define custom properties
        self._cacheLock = ReadWriteLock()
        self._cache = {}
        self._expiresAt = {}

    def _cleanup(self):
        """
        Cleans up any expired keys from the cache.
        """
        now = datetime.datetime.now()
        with WriteLocker(self._cacheLock):
            for key, expires in self._expiresAt.items():
                if expires < now:
                    self._expiresAt.pop(key, None)
                    self._cache.pop(key, None)

    def expire(self, key=None):
        """
        Expires the given key from the local cache.

        :param      key | <hashable>
        """
        with WriteLocker(self._cacheLock):
            if key:
                self._cache.pop(key, None)
                self._expiresAt.pop(key, None)
            else:
                self._cache.clear()
                self._expiresAt.clear()

    def isCached(self, key):
        """
        Returns whether or not the inputted key is cached.

        :param      key | <hashable>

        :return     <bool>
        """
        if not self.isEnabled():
            return False

        self._cleanup()

        with ReadLocker(self._cacheLock):
            return key in self._cache

    def setValue(self, key, value, timeout=None):
        """
        Caches the inputted key and value to this instance.

        :param      key     | <hashable>
                    value   | <variant>
        """
        if not self.isEnabled():
            return

        timeout = timeout or self.timeout()

        with WriteLocker(self._cacheLock):
            self._cache[key] = value
            self._expiresAt[key] = datetime.datetime.now() + datetime.timedelta(seconds=timeout)

    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.

        :return     <variant>
        """
        with ReadLocker(self._cacheLock):
            return self._cache.get(key)

# create a basic cache object
DataCache.registerAddon('Basic', BasicCache)