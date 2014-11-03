#!/usr/bin/python

""" Defines an indexing system to use when looking up records. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

import datetime
import logging
import projex.security

from xml.etree import ElementTree

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr


log = logging.getLogger(__name__)
orb = LazyModule('orb')
errors = LazyModule('orb.errors')


class Index(object):
    """ 
    Defines an indexed way to lookup information from a database.
    Creating an Index generates an object that works like a method, however
    has a preset query built into it, along with caching options.
    """
    def __init__(self, 
                 name='', 
                 columns=None, 
                 unique=False, 
                 order=None, 
                 cached=False,
                 referenced=False):
        
        if columns == None:
            columns = []
        
        self.__name__ = name
        self._columnNames = columns
        self._unique = unique
        self._order = order
        self._cache = {}
        self._cached = cached
        self._cachedExpires = 0
        self._referenced = referenced
    
    def __call__(self, table, *values, **options):
        # make sure we have the right number of arguments
        if len(values) != len(self._columnNames):
            name = self.__name__
            columnCount = len(self._columnNames)
            valueCount = len(values)
            opts = (name, columnCount, valueCount)
            text = '%s() takes exactly %i arguments (%i given)' % opts
            raise TypeError(text)
        
        data = tuple(hash(value) for value in values)
        cache_key = (data,
                     hash(orb.LookupOptions(**options)),
                     id(options.get('db')))
        cache = self.cache(table)
        
        if cache and not cache.isExpired(cache_key):
            return cache.value(cache_key)

        # create the lookup query
        query = orb.Query()
        for i, col in enumerate(self._columnNames):
            value = values[i]
            column = table.schema().column(col)
            
            if orb.Table.recordcheck(value) and not value.isRecord():
                if self._unique:
                    return None
                
                return orb.RecordSet()
            
            if not column:
                tableName = table.schema().name()
                raise errors.ColumnNotFound(col, tableName)
            
            if column.isEncrypted():
                value = projex.security.encrypt(value)
                
            query &= orb.Query(col) == value
        
        # include additional where option selection
        if 'where' in options:
            options['where'] = query & options['where']
        else:
            options['where'] = query
        
        order   = options.get('order', self._order)
        columns = options.get('columns', None)
        
        # selects the records from the database
        if self._unique:
            results = table.selectFirst(**options)
        else:
            results = table.select(**options)
        
        # cache the results
        if cache and results is not None:
            cache.setValue(cache_key, results)
        
        return results
    
    def cache(self, table):
        """
        Returns the cache associated with this index for the given table.
        
        :return     <orb.TableCache> || None
        """
        if not self.cached():
            return None
        
        if not table in self._cache:
            cache = orb.TableCache(table, self._cachedExpires)
            self._cache[table] = cache
        
        return self._cache[table]
    
    def cached(self):
        """
        Returns whether or not the results for this index should be cached.
        
        :return     <bool>
        """
        return self._cached
    
    def cachedExpires(self):
        """
        Returns the number of seconds that this index will keep its cache
        before reloading.
        
        :return     <int> | seconds
        """
        return self._cachedExpires
    
    def columnNames(self):
        """
        Returns the list of column names that this index will be expecting as \
        inputs when it is called.
        
        :return     [<str>, ..]
        """
        return self._columnNames
    
    def isReferenced(self):
        """
        Returns whether or not this
        """
        return self._referenced
    
    def name(self):
        """
        Returns the name of this index.
        
        :return     <str>
        """
        return self.__name__
    
    def setCached(self, state):
        """
        Sets whether or not this index should cache the results of its query.
        
        :param      state | <bool>
        """
        self._cached = state
    
    def setCachedExpires(self, seconds):
        """
        Sets the time in seconds that this index will hold onto a client
        side cache.  If the value is 0, then the cache will never expire, 
        otheriwise it will update after N seconds.
        
        :param     seconds | <int>
        """
        self._cachedExpires = seconds
    
    def setColumnNames(self, columnNames):
        """
        Sets the list of the column names that this index will use when \
        looking of the records.
        
        :param      columnNames | [<str>, ..]
        """
        self._columnNames = columnNames
    
    def setOrder(self, order):
        """
        Sets the order information for this index for how to sort and \
        organize the looked up data.
        
        :param      order   | [(<str> field, <str> direction), ..]
        """
        self._order = order
    
    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name__ = nstr(name)
    
    def setUnique(self, state):
        """
        Sets whether or not this index should find only a unique record.
        
        :param      state | <bool>
        """
        self._unique = state
    
    def unique(self):
        """
        Returns whether or not the results that this index expects should be \
        a unique record, or multiple records.
        
        :return     <bool>
        """
        return self._unique

    def toolTip(self):
        return '<h1>{0} <small>Index</small></h1>'.format(self.name())

    def toXml(self, xparent):
        """
        Saves the index data for this column to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xindex = ElementTree.SubElement(xparent, 'index')
        xindex.set('name', self.name())
        xindex.set('columns', ','.join(self.columnNames()))
        xindex.set('unique', nstr(self.unique()))
        xindex.set('cached', nstr(self.cached()))
        xindex.set('cachedExpires', nstr(self._cachedExpires))
        
        return xindex
    
    @staticmethod
    def fromXml(xindex, referenced=False):
        """
        Generates an index method descriptor from xml data.
        
        :param      xindex  | <xml.etree.Element>
        
        :return     <Index> || None
        """
        index = Index(referenced=referenced)
        index.setName(xindex.get('name', ''))
        index.setColumnNames(xindex.get('columns', '').split(','))
        index.setUnique(xindex.get('unique') == 'True')
        index.setCached(xindex.get('cached') == 'True')
        index.setCachedExpires(int(xindex.get('cachedExpires',
                               index._cachedExpires)))
        
        return index

