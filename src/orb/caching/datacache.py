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

import datetime

from projex.lazymodule import LazyModule

orb = LazyModule('orb')


class DataCache(object):
    """ Base caching object for tracking data caches """
    def __init__(self, expires=0):
        self._expires = expires # seconds
        self._enabled = True
        self._cache = {}
        self._cachedAt = {}
    
    def cachedAt(self, key):
        """
        Returns when the inputed key was last cached for this instance.
        
        :param      key | <hashable>
        """
        return self._cachedAt.get(key)
    
    def clear(self):
        """
        Clears out all the caching information for this instance.
        """
        self._cachedAt.clear()
        self._cache.clear()
    
    def expires(self):
        """
        Returns the number of seconds that this cache will store its
        information before it expires.
        
        :return     <int> | seconds
        """
        return self._expires
    
    def isCached(self, key):
        """
        Returns whether or not the inputed key is cached.
        
        :param      key | <hashable>
        
        :return     <bool>
        """
        return key in self._cache
    
    def isEnabled(self):
        """
        Returns whether or not this cache instance is enabled.  You can
        disable local cache instances by calling setEnabled, or global
        caching from the manager instance.
        
        :return     <bool>
        """
        return self._enabled and orb.system.isCachingEnabled()
    
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
        max_expires = orb.system.maxCacheTimeout() * 60
        if self.expires() == 0:
            expires = max_expires
        else:
            expires = min(self.expires(), max_expires)
        
        now  = datetime.datetime.now()
        secs = (now - cachedAt).total_seconds()
        
        return expires < secs
    
    def setEnabled(self, state):
        """
        Sets whether or not the caching for this instance is enabled.
        
        :param      state | <bool>
        """
        self._enabled = state

    def setExpires(self, seconds):
        """
        Sets the number of seconds that this cache will store its information
        before it expires.
        
        :param      seconds | <int>
        """
        self._expires = seconds

    def setValue(self, key, value):
        """
        Caches the inputed key and value to this instance.
        
        :param      key     | <hashable>
                    value   | <variant>
        """
        self._cache[key] = value
        self._cachedAt[key] = datetime.datetime.now()
    
    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.
        
        :return     <variant>
        """
        return self._cache.get(key)

