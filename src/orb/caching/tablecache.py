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


class TableCache(DataCache):
    """" Base class for referencing orb Table cache information """
    
    def __init__(self, table, expires=0):
        # determine the expire information
        table_expires = table.schema().cacheExpireIn() * 60
        max_expires = orb.system.maxCacheTimeout() * 60
        opts = [expires, table_expires, max_expires]
        expires = min(filter(lambda x: x > 0, opts))
        
        # initialize the table cache
        super(TableCache, self).__init__(expires)
        
        # define custom properties
        self._table = table
        self._preloaded = table.schema().preloadCache()
    
    def isExpired(self, key):
        """
        Returns whether or not the given key is expired for this
        caching instance.  This will also take into account any table
        record changes that will affect caching.
        
        :sa     Table.markTableCacheExpired
        
        :param      key | <hashable>
        """
        cachedAt = self.cachedAt(key)
        if cachedAt is None:
            return True
        
        if self.table().isTableCacheExpired(cachedAt):
            return True
        
        return super(TableCache, self).isExpired(key)
    
    def isPreloaded(self):
        """
        Returns whether or not the cache is preloaded.
        
        :return     <bool>
        """
        return self._preloaded
    
    def setPreloaded(self, state):
        """
        Sets whether or not the cache should be preloaded.
        
        :param      state | <bool>
        """
        self._preloaded = state
    
    def table(self):
        """
        Returns the table associated with this cache instance.
        
        :return     <orb.Table>
        """
        return self._table
