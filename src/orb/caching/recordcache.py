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

import logging

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)
orb = LazyModule('orb')

class OrderCompare(object):
    """ Defines a class for comparing records by a database order scheme """
    def __init__(self, order):
        self._order = order
    
    def __call__(self, a, b):
        """
        Compares the inputed values for each column based on a given
        direction.
        
        :param      a | <dict>
                    b | <dict>
        
        :return     <int> -1 || 0 || 1
        """
        for col, direction in self._order:
            a_val = a.get(col)
            b_val = b.get(col)
            
            # ignore same results
            result = cmp(a_val, b_val)
            if not result:
                continue
            
            # return the direction result
            if direction == 'desc':
                return -result
            else:
                return result
        return 0

#----------------------------------------------------------------------

class RecordCache(object):
    """
    Defines a key cache class for the table to use when caching records from the
    system.  Caching is defined on the TableSchema class, see 
    TableSchema.setCachingEnabled for more info.
    
    :usage      |from orb import RecordCache
                |
                |with RecordCache(User, AccountType):
                |   for transaction in Transactions.select():
                |       print transaction.transferredBy()  # lookups up from
                |                                          # the user cache
                
                |from orb import Table
                |
                |class User(Table):
                |   __db_cached__ = True
                |
                |User.select() # looks up from the RecordCache for the table
    """
    def __init__(self, *tables, **kwds):
        if not tables:
            tables = orb.system.models()
        
        expires = kwds.get('expires', 0)
        self._caches = dict([(table, orb.TableCache(table, expires)) \
                             for table in tables])
    
    def __enter__(self):
        self.begin()
    
    def __exit__(self, *args):
        self.end()
    
    def begin(self):
        """
        Begins the caching process for this instance.
        """
        for table in self._caches:
            table.pushRecordCache(self)
    
    def cache(self, table):
        """
        Returns the cache associated with this record cache for the given
        table.
        
        :return     <orb.TableCache> || None
        """
        return self._caches.get(table)
    
    def clear(self, table=None):
        """
        Clears the current cache information.
        """
        if table:
            cache = self._caches.get(table)
            if cache:
                cache.clear()
        else:
            for cache in self._caches.values():
                cache.clear()
    
    def count(self, backend, table, lookup, options):
        """
        Returns the number of entries based on the given options.
        
        :param      backend | <orb.Connection>
                    table   | subclass of <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
        
        :return     <int>
        """
        options.inflated = False
        return len(self.select(backend, table, lookup, options))
    
    def distinct(self, backend, table, lookup, options):
        """
        Returns a distinct set o fentries based on the given lookup options.
        
        :param      table   | subclass of <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
        
        :return     {<str> columnName: <list> value, ..}
        """
        columns = list(lookup.columns)
        output = dict([(column, set()) for column in columns])
        for record in self.select(backend, table, lookup, options):
            for column in columns:
                output[column].add(record.get(column))
        
        for key, value in output.items():
            output[key] = list(value)
        
        return output
    
    def end(self):
        """
        Ends the caching process for this instance.
        """
        for table in self._caches:
            table.popRecordCache()

    def isExpired(self, table, key):
        """
        Returns whether or not this cache is expired for the given table
        and key.
        
        :param      table | subclass of <orb.Table>
                    key   | <hashable>
        
        :return     <bool>
        """
        cache = self._caches.get(table)
        if not cache:
            return True
        
        return cache.isExpired(key)

    def preloadedRecords(self, table, lookup):
        """
        Looking up pre-loaded record information.
        
        :param      table  | subclass of <orb.Table>
                    lookup | <orb.LookupOptions>
        
        :return     [<dict> data, ..]
        """
        cache = self.cache(table)
        if not cache:
            return []
        
        records = cache.value('preloaded_records', [])
        if lookup.order:
            records = sorted(records, OrderCompare(lookup.order))

        start = lookup.start or 0
        offset = 0
        output = []

        schema = table.schema()
        columns = schema.columns() if not lookup.columns else [schema.column(col) for col in lookup.columns]

        for r, record in enumerate(records):
            # ensure we're looking up a valid record
            if lookup.where and not lookup.where.validate(record, table):
                continue

            if start < offset:
                offset += 1
                continue

            # ensure we have unique ordering for distinct
            record = [item for item in record.items() if schema.column(item[0]) in columns]
            record.sort()
            if lookup.distinct and record in output:
                continue
            
            output.append(dict(record))
            if lookup.limit and len(output) == lookup.limit:
                break
        
        return output
    
    def record(self, table, primaryKey):
        """
        Returns a record for the given primary key.
        
        :return     {<str> columnName: <variant> value, ..} record
        """
        lookup = orb.LookupOptions()
        lookup.where = Q(table) == primaryKey
        options = orb.DatabaseOptions()
        return self.selectFirst(table.getDatabase().backend(),
                                table,
                                lookup,
                                options)
    
    def records(self, table, **options):
        """
        Returns a list of records that are cached.
        
        :param      table | subclass of <orb.Table>
                    query | <orb.Query> || None
        
        :return     [<Table>, ..]
        """
        if 'query' in options:
            options['where'] = options.pop('query')
        
        lookup = orb.LookupOptions(**options)
        db_opts = orb.DatabaseOptions(**options)
        return self.select(table.getDatabase().backend(),
                           table,
                           lookup,
                           db_opts)
    
    def setExpires(self, table, minutes):
        """
        Sets the length of time in minutes to hold onto this cache before 
        re-querying the database.
        
        :param      table   | subclass of <orb.Table>
                    minutes | <int> || <float>
        """
        cache = self.cache(table)
        if cache:
            expires = minutes * 60
            table_expires = table.schema().cacheExpireIn() * 60
            max_expires = orb.system.maxCacheTimeout() * 60
            opts = [expires, table_expires, max_expires]
            expires = min(filter(lambda x: x > 0, opts))
            
            cache.setExpires(expires)
    
    def selectFirst(self, backend, table, lookup, options):
        """
        Returns a list of records from the cache that matches the inputed
        parameters.
        
        :param      table   | subclass of <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
        
        :return     <dict> record || None
        """
        lookup.limit = 1
        results = self.select(backend, table, lookup, options)
        if results:
            return results[0]
        return None
    
    def select(self, backend, table, lookup, options):
        """
        Returns a list of records from the cache that matches the inputed
        parameters.
        
        :param      backend | <orb.Connection>
                    table   | subclass of <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
        
        :return     [<dict> record, ..]
        """
        cache = self.cache(table)
        cache_key = (hash(lookup), hash(options), id(backend.database()))
        preload_key = 'preloaded_records'
        
        # determine if the query is simple (only involving a simple table)
        # if it is, we can use local querying on the cached set for 
        # preloaded records.  If it is not (joined tables) we need to
        # actually query the database and cache the results for this query
        if lookup.where is not None:
            is_simple = len(lookup.where.tables()) <= 1
        else:
            is_simple = not (bool(lookup.expand) or options.locale != orb.system.locale())
        
        # return an exact cached match
        if cache and not cache.isExpired(cache_key):
            return cache.value(cache_key)
        
        # return a parsed match from preloaded records
        elif is_simple and cache and not cache.isExpired(preload_key):
            records = self.preloadedRecords(table, lookup)
            cache.setValue(cache_key, records)
            return records
        
        # otherwise, determine if we need to load this exact search
        # or reload the pre-loaded records
        elif is_simple and cache and cache.isPreloaded():
            all_lookup = orb.LookupOptions()
            all_opts   = orb.DatabaseOptions()

            records = backend.select(table, all_lookup, all_opts)
            cache.setValue(preload_key, records)

            records = self.preloadedRecords(table, lookup)
            cache.setValue(cache_key, records)
            return records
        
        # otherwise, search the backend for this lookup specifically and
        # cache the results
        else:
            records = backend.select(table, lookup, options)
            if cache:
                cache.setValue(cache_key, records)
            
            return records

    def tables(self):
        """
        Returns the tables that are associated with this cache instance.
        
        :return     [subclass of <orb.Table>, ..]
        """
        return self._caches.keys()
