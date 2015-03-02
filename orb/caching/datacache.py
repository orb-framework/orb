"""
Defines caching methods to use when working with tables.
"""

import datetime
import projex

from projex.decorators import abstractmethod
from projex.addon import AddonManager
from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class DataCache(AddonManager):
    """ Base caching object for tracking data caches """
    def __init__(self, timeout=0):
        self._timeout = timeout
        self._enabled = True

    def __getitem__(self, key):
        if self.isCached(key):
            return self.value(key)
        else:
            raise KeyError(key)

    def __setitem__(self, key, value):
        self.setValue(key, value)

    # noinspection PyUnusedLocal
    @abstractmethod()
    def cachedAt(self, key):
        """
        Returns when the inputted key was last cached for this instance.
        
        :param      key | <hashable>

        :return     <datetime>
        """
        return datetime.datetime.min

    def clear(self):
        """
        Clears out all the caching information for this instance.
        """
        return self.expire()

    @abstractmethod()
    def expire(self, key=None):
        """
        Removes the given key or clears the cache for this data cache.

        :param      key | <hashable> || None
        """
        return False

    @abstractmethod()
    def isCached(self, key):
        """
        Returns whether or not the inputted key is cached.
        
        :param      key | <hashable>
        
        :return     <bool>
        """
        return False

    def isEnabled(self):
        """
        Returns whether or not this cache instance is enabled.  You can
        disable local cache instances by calling setEnabled, or global
        caching from the manager instance.
        
        :return     <bool>
        """
        return self._enabled and orb.system.isCachingEnabled()

    def setEnabled(self, state):
        """
        Sets whether or not the caching for this instance is enabled.
        
        :param      state | <bool>
        """
        self._enabled = state

    def setTimeout(self, timeout):
        """
        Sets the number of milliseconds that this cache will store its information
        before it expires.
        
        :param      timeout | <int> | milliseconds
        """
        self._timeout = timeout

    @abstractmethod()
    def setValue(self, key, value, timeout=0):
        """
        Caches the inputted key and value to this instance.
        
        :param      key     | <hashable>
                    value   | <variant>
        """
        pass

    def timeout(self):
        """
        Returns the number of seconds that this cache will store its
        information before it timeout.

        :return     <int> | seconds
        """
        return self._timeout or orb.system.maxCacheTimeout()

    @abstractmethod()
    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.
        
        :return     <variant>
        """
        return default

    @staticmethod
    def create(timeout=0):
        """
        Creates a new data cache instance through the orb system manager.  To setup a factory, use
        `orb.system.setCacheFactory(factory)`

        :return     <orb.caching.DataCache>
        """
        factory = orb.system.cacheFactory()
        if factory:
            if isinstance(factory, orb.DataCache):
                return factory
            elif callable(factory):
                return factory(timeout)
            else:
                cls = DataCache.byName(factory)
                return cls(timeout)
        else:
            cls = DataCache.byName('Basic')
            return cls(timeout)

from orb.caching.backends import __plugins__
projex.importmodules(__plugins__)