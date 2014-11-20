""" 
Creates a class for manipulating groups of records at once. 
This is useful when you need to remove a lot of records at one time, and is the
return result from the select mechanism that supports paging.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2012, Projex Software'
__license__         = 'LGPL'

# maintenance information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

import logging
import projex.iters
import re

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from projex.rest import unjsonify

from ..common import SearchMode

log = logging.getLogger(__name__)
orb = LazyModule('orb')
errors = LazyModule('orb.errors')


#------------------------------------------------------------------------------

class RecordSet(object):
    """
    Defines a class that can be used to manipulate table records in bulk.  For
    more documentation on the RecordSet, check out the 
    [[$ROOT/recordsets|record set documentation]].
    """
    def __init__(self, *args):
        self._table             = None
        
        # default options
        self._grouped           = False
        self._ordered           = False
        self._inflated          = None
        self._counts            = {}
        self._empty             = {}
        self._currentPage       = -1
        self._pageSize          = 0
        self._locale            = None
        
        # sorting options
        self._sort_cmp_callable = None
        self._sort_key_callable = None
        self._sort_reversed     = False
        
        # record cache
        self._all               = {}
        self._length            = None
        
        # select information
        self._expand            = None
        self._start             = None
        self._limit             = None
        self._database          = -1
        self._query             = None
        self._columns           = None
        self._groupBy           = None
        self._order             = None
        self._ignoreAggregates  = False
        self._ignoreJoined      = False
        self._namespace         = None
        
        # join another record set as RecordSet(recordSet)
        if args:
            data = args[0]
            
            if RecordSet.typecheck(data):
                self.duplicate(data)
                
            # join a list of records as RecordSet([record, record, record])
            elif type(data) in (list, tuple):
                self._all[None] = data[:]
                if data:
                    self._table = type(data[0])
            
            # assign a table as the record set RecordSet(orb.Table)
            elif orb.Table.typecheck(data):
                self._table = data
                
                if len(args) > 1 and type(args[1]) in (list, tuple):
                    self._all[None] = args[1][:]
    
    def __len__(self):
        """
        The length of this item will be its total count.
        
        :return     <int>
        """
        # return the length of the all records cache
        if None in self._all:
            return len(self._all[None])
        
        # return 0 for null record sets
        elif self.isNull():
            return 0
        
        # collect the length from the database
        elif self._length == None:
            self._length = self.count()
            
        return self._length
    
    def __iter__(self):
        """
        Return the fully resolved list of data.
        
        :return     <list>
        """
        return iter(self.all())
    
    def __add__(self, other):
        """
        Adds two record set instances together and returns them.
        
        :param      other | <RecordSet> || <list>
        """
        return RecordSet(self).join(other)
    
    def __getitem__(self, value):
        """
        Returns the item at the inputed index for this record set.
        
        :return     <orb.Table>
        """
        # slice up this record set based on the inputed information
        if type(value) == slice:
            rset = RecordSet(self)
            
            # set the start value for this record set
            if value.start is not None:
                if self.start() is not None:
                    rset.setStart(self.start() + value.start)
                else:
                    rset.setStart(value.start)
            
            # set the limit value for this record set
            if value.stop is not None:
                if value.start is not None:
                    rset.setLimit(value.stop - value.start)
                else:
                    rset.setLimit(value.stop)
            
            # slice up the records
            if None in rset._all:
                rset._all[None] = rset._all[None][value]
            
            return rset
        
        # return the record from a given index
        else:
            return self.recordAt(value)
        
    def __nonzero__( self ):
        """
        Returns whether or not this record set is a non-zero entry.
        
        :return     <bool>
        """
        return not self.isEmpty()
    
    def _schemaBreakdown(self, **options):
        """
        Returns this recordset broken down by its schema into each record.
        If primaryOnly is set to True, then only the primary keys will be
        grouped together, otherwise ful records will be.
        
        :param      primaryOnly | <bool>
        
        :return     [<orb.Table>: <orb.Query>, ..}
        """
        # return the breakdown from the cache
        if not options and None in self._all:
            output = {}
            for record in self._all[None]:
                table = type(record)
                output.setdefault(table, set())
                output[table].add(record.primaryKey())
            
            return {table: orb.Query(table).in_(keys) \
                    for table, keys in output.items()}
        
        else:
            lookup = self.lookupOptions(**options)
            return {self.table(): lookup.where}
    
    def all(self, **options):
        """
        Looks up all the records based on the current query information.
        Additional options can include any options available for the 
        <orb.LookupOptions> or <orb.DatabaseOptions> classes that will be passed
        to this recordset backend.
        
        :return     [<orb.Table>, ..]
        """
        table = self.table()
        db  = self.database()
        
        lookup  = self.lookupOptions(**options)
        db_opts = self.databaseOptions(**options)
        
        key = self.cacheKey(options)
        if self.isNull():
            return []
        
        # return the cached lookups
        if key in self._all:
            output = self._all[key]
        else:
            # create the lookup information
            cache = table.recordCache()
            
            # grab the database backend
            backend = db.backend()
            if not backend:
                return []
            
            # cache the result for this query
            results = None
            if cache is not None and orb.system.isCachingEnabled():
                results = cache.select(backend, table, lookup, db_opts)
            else:
                try:
                    results = backend.select(table, lookup, db_opts)
                except errors.OrbError, err:
                    if db_opts.throwErrors:
                        raise
                    else:
                        log.error('Backend error occurred.\n%s', err)
                        results = []
            
            # return specific columns
            if db_opts.inflateRecords:
                output = map(lambda x: self.inflateRecord(table, x), results)
            
            elif lookup.columns and not options.get('ignoreColumns'):
                if len(lookup.columns) == 1:
                    col = lookup.columns[0]
                    output = [x[col] for x in results]
                else:
                    output = [[r.get(col, None) for col in lookup.columns]
                              for r in results]
            
            # return the raw results
            else:
                output = results

            self._all[key] = output
        
        # return sorted results from an in-place sort
        if self._sort_cmp_callable is not None or \
           self._sort_key_callable is not None:
            return sorted(output, 
                          cmp = self._sort_cmp_callable,
                          key = self._sort_key_callable,
                          reverse = self._sort_reversed)
        return output
    
    def cacheKey(self, options):
        """
        Returns the cache key for based on the inputed dictionary of
        options.
        
        :param      options | <dict>
        
        :return     <hash>
        """
        if not options:
            return None
        else:
            return hash(orb.LookupOptions(**options))
    
    def clear(self):
        """
        Clears all the cached information such as the all() records,
        records, and total numbers.
        """
        self._all.clear()
        self._length = None
        self._counts.clear()
        self._empty.clear()
    
    def commit(self, **options):
        """
        Commits the records associated with this record set to the database.
        """
        db = options.pop('db', self.database())
        backend = db.backend()
        
        # insert new records
        inserts = []
        updates = []
        
        lookup = self.lookupOptions(**options)
        db_options = self.databaseOptions(**options)
        records = self.all(**options)
        
        # run each records pre-commit logic before it is inserted to the db
        for record in records:
            record.preCommit(**options)
        
        inserts = filter(lambda x: not x.isRecord(db=db), records)
        updates = filter(lambda x: x.isRecord(db=db), records)
        
        try:
            if inserts:
                backend.insert(inserts, lookup, db_options)
            if updates:
                backend.update(updates, lookup, db_options)
        except errors.OrbError, err:
            if db_options.throwErrors:
                raise
            else:
                log.error('Backend error occurred.\n%s', err)
                return False
        
        # update the caching table information
        for table in set(map(type, records)):
            table.markTableCacheExpired()
        
        # run each records pre-commit logic before it is inserted to the db
        for record in records:
            record.postCommit(**options)
        
        return True
    
    def count(self, **options):
        """
        Collects the count of records for this record set.  Additional options
        can include any options available for the <orb.LookupOptions> or 
        <orb.DatabaseOptions> classes that will be passed to this records
        backend.
        
        :return     <int>
        """
        table = self.table()
        db = self.database()
        
        if self.isNull():
            return 0
        
        key = self.cacheKey(options)
        lookup  = self.lookupOptions(**options)
        options = self.databaseOptions(**options)
        
        if key in self._counts:
            return self._counts[key]
        
        # retrieve the count information
        cache = table.recordCache()
        if cache is not None and orb.system.isCachingEnabled():
            count = cache.count(db.backend(), table, lookup, options)
        else:
            try:
                count = db.backend().count(table, lookup, options)
            except errors.OrbError, err:
                if options.throwErrors:
                    raise
                else:
                    log.error('Backend error occurred.\n%s', err)
                    return 0
        
        self._counts[key] = count
        return count
    
    def columns(self):
        """
        Returns the columns that this record set should be querying for.
        
        :return     [<str>, ..] || None
        """
        return self._columns

    def currentPage(self):
        """
        Returns the current page that this record set represents.

        :return     <int>
        """
        return self._currentPage

    def database(self):
        """
        Returns the database instance that this recordset will use.
        
        :return     <Database>
        """
        if self._database != -1:
            return self._database
        
        if self.table():
            db = self.table().getDatabase()
        else:
            db = orb.system.database()
            
        if not db:
            raise errors.DatabaseNotFound()
        
        self._database = db
        return db
    
    def databaseOptions(self, **options):
        """
        Returns the database options for this record set.  See the
        <orb.DatabaseOptions> documentation about the optional arguments.
        
        :return     <orb.DatabaseOptions>
        """
        options.setdefault('inflateRecords', self.isInflated())
        options.setdefault('namespace', self.namespace())
        options.setdefault('locale', self._locale)
        
        return orb.DatabaseOptions(**options)
    
    def duplicate(self, other):
        """
        Duplicates the data from the other record set instance.
        
        :param      other | <RecordSet>
        """
        # default options
        self._grouped           = other._grouped
        self._ordered           = other._ordered
        self._inflated          = other._inflated
        self._counts            = other._counts.copy()
        self._empty             = other._empty.copy()
        self._currentPage       = other._currentPage
        self._pageSize          = other._pageSize
        self._locale            = other._locale

        # sorting options
        self._sort_cmp_callable = other._sort_cmp_callable
        self._sort_key_callable = other._sort_key_callable
        self._sort_reversed     = other._sort_reversed

        # record cache
        self._all               = other._all.copy()
        self._length            = other._length

        # select information
        self._expand            = list(other._expand) if other._expand else None
        self._start             = other._start
        self._limit             = other._limit
        self._database          = other._database
        self._query             = other._query.copy() if other._query else None
        self._columns           = list(other._columns) if other._columns else None
        self._groupBy           = list(other._groupBy) if other._groupBy else None
        self._order             = list(other._order) if other._order else None
        self._ignoreAggregates  = other._ignoreAggregates
        self._ignoreJoined      = other._ignoreJoined
        self._namespace         = other._namespace

        self._table             = other._table

    def distinct(self, columns, **options):
        """
        Returns a distinct series of column values for the current record set
        based on the inputed column list.
        
        The additional options are any keyword arguments supported by the
        <orb.LookupOptions> or <orb.DatabaseOptions> classes.  
        
        If there is one column supplied, then the result will be a list, 
        otherwise, the result will be a dictionary.
        
        :param      column   | <str> || [<str>, ..]
                    **options
        
        :return     [<variant>, ..] || {<str> column: [<variant>, ..]}
        """
        # ensure we have a list of values
        if not type(columns) in (list, tuple):
            columns = [nstr(columns)]
        
        table = self.table()
        db  = self.database()
        
        # ensure we have a database and table class
        if self.isNull():
            if len(columns) > 1:
                return {}
            return []
        
        # return information from the database
        cache  = table.recordCache()
        schema = table.schema()
        
        backend = db.backend()
        lookup  = self.lookupOptions(**options)
        options = self.databaseOptions(**options)
        
        lookup.columns = list(columns)
        
        if cache is not None and orb.system.isCachingEnabled():
            output = cache.distinct(backend, table, lookup, options)
        else:
            try:
                output = backend.distinct(table, lookup, options)
            except errors.OrbError, err:
                if options.throwErrors:
                    raise
                else:
                    log.error('Backend error occurred.\n%s', err)
                    output = {}
        
        if options.inflateRecords:
            for key in output.keys():
                column = schema.column(key)
                if column and column.isReference():
                    ref_model = column.referenceModel()
                    if not ref_model:
                        msg = '%s is not a valid model.'
                        log.error(msg, column.reference())
                        continue
                    
                    ids = output[key]
                    records = []
                    if None in ids:
                        ids.remove(None)
                        records.append(None)
                    
                    if ids:
                        q = orb.Query(ref_model).in_(ids)
                        records += list(ref_model.select(where=q))
                    
                    output[key] = records
        
        if len(columns) == 1:
            return output.get(columns[0], [])
        return output
        
    def first(self, **options):
        """
        Returns the first record that matches the current query.
        
        :return     <orb.Table> || None
        """
        table = self.table()
        db = self.database()
        
        if self.isNull():
            return None
        
        options['limit'] = 1
        
        lookup       = self.lookupOptions(**options)
        db_opts      = self.databaseOptions(**options)
        
        # retrieve the data from the cache
        cache = table.recordCache()
        if cache is not None and orb.system.isCachingEnabled():
            records = cache.select(db.backend(), table, lookup, db_opts)
        else:
            try:
                records = db.backend().select(table, lookup, db_opts)
            except errors.OrbError, err:
                if db_opts.throwErrors:
                    raise
                else:
                    log.error('Backend error occurred.\n%s', err)
                    records = []
        
        if records:
            if db_opts.inflateRecords:
                return self.inflateRecord(table, records[0])
            return records[0]
        return None
        
    def groupBy(self):
        """
        Returns the grouping information for this record set.
        
        :return     [<str>, ..] || None
        """
        return self._groupBy
    
    def grouped(self, grouping=None, **options):
        """
        Returns the records in a particular grouping.  If the groupBy option
        is left as None, then the base grouping for this instance will be used.
        
        :param      groupBy | <str> columnName || [<str> columnName, ..] || None
        
        :return     { <variant> grouping: <orb.RecordSet>, .. }
        """
        if grouping is None:
            grouping = self.groupBy()
        
        if not type(grouping) in (list, tuple):
            grouping = [grouping]
        
        table  = self.table()
        output = {}
        
        if grouping:
            grp_format = grouping[0]
            remain = grouping[1:]
            
            # look for advanced grouping
            if '{' in grp_format:
                formatted = True
                columns = list(set(re.findall('\{(\w+)', grp_format)))
            else:
                formatted = False
                columns = [grp_format]
            
            values = self.distinct(columns, **options)
            
            for value in values:
                lookup      = self.lookupOptions(**options)
                db_options  = self.databaseOptions(**options)
                
                # generate the sub-grouping query
                options = {}
                if len(columns) > 1:
                    sub_query = orb.Query()
                    for i, column in enumerate(columns):
                        sub_query &= orb.Query(column) == value[i]
                        options[column] = value[i]
                else:
                    sub_query = orb.Query(columns[0]) == value
                    options[columns[0]] = value
                
                # define the formatting options
                if formatted:
                    key = grp_format.format(**options)
                else:
                    key = value
                
                # assign or merge the output for the grouping
                if key in output:
                    sub_set = output[key]
                    sub_set.setQuery(sub_set.query() | sub_query)
                else:
                    sub_set = RecordSet(table)
                    lookup = self.lookupOptions(**options)
                    db_options = self.databaseOptions(**options)
                
                    # join the lookup options
                    if lookup.where is None:
                        lookup.where = sub_query
                    else:
                        lookup.where &= sub_query
                    
                    sub_set.setLookupOptions(lookup)
                    sub_set.setDatabaseOptions(db_options)
                    
                    if remain:
                        sub_set = sub_set.grouped(remain, **options)
                    
                    output[key] = sub_set
        
        return output

    def ids(self, **options):
        """
        Returns a list of the ids that are associated with this record set.

        :sa         primaryKeys

        :return     [<variant>, ..]
        """
        return self.primaryKeys(**options)

    def inflateRecord(self, table, record):
        """
        Inflates the record for the given class, applying a namespace override
        if this record set is using a particular namespace.
        
        :param      table     | <subclass of orb.Table>
                    record  | <dict>
        
        :return     <orb.Table>
        """
        inst = table.inflateRecord(record, record, db=self.database())
        if self._namespace is not None:
            inst.setRecordNamespace(self._namespace)
        return inst
    
    def index(self, record):
        """
        Returns the index of the inputed record within the all list.
        
        :param      record | <orb.Table>
        
        :return     <int>
        """
        if not record:
            return -1
        elif None in self._all:
            return self._all[None].index(record)
        else:
            return self.primaryKeys().index(record.primaryKey())
    
    def indexed(self, columns=None, **options):
        """
        Returns the records in a particular grouping.  If the groupBy option
        is left as None, then the base grouping for this instance will be used.
        
        :param      columns | <str> columnName || [<str> columnName, ..]
        
        :return     { <variant> grouping: <orb.Table>, .. }
        """
        if columns == None:
            return dict([(x.primaryKey(), x) for x in self.all(**options)])
            
        if not type(columns) in (list, tuple):
            columns = [columns]
        
        table  = self.table()
        output = {}
        
        auto = options.pop('indexInflated', True)
        
        if columns:
            for record in self.all(**options):
                key = []
                for column in columns:
                    key.append(record.recordValue(column, autoInflate=auto))
                    
                if len(key) == 1:
                    output[key[0]] = record
                else:
                    output[tuple(key)] = record
        
        return output
    
    def insertInto(self, db, **options):
        """
        Inserts these records into another database.  This method will allow
        for easy duplication of one record in one database to another database.
        
        :param      db | <orb.Database>
        """
        if self.database() == db:
            return False
        
        lookup = orb.LookupOptions(**options)
        db_opts = orb.DatabaseOptions(**options)
        db_opts.force = True
        
        backend = db.backend()
        records = self.all()
        try:
            backend.insert(records, lookup, db_opts)
            return True
        except errors.OrbError, err:
            if db_opts.throwErrors:
                raise
            else:
                log.error('Backend error occurred.\n%s', err)
                return False

    def ignoreAggregates(self):
        """
        Returns whether or not aggregates should be included by default in the
        queries.
        
        :return     <bool>
        """
        return self._ignoreAggregates

    def ignoreJoined(self):
        """
        Returns whether or not joined columns should be included by default in
        the queries.
        
        :return     <bool>
        """
        return self._ignoreJoined

    def isEmpty(self, **options):
        """
        Returns whether or not this record set contains any records.
        
        :return     <bool>
        """
        if self.isNull():
            return True
        
        if None in self._all:
            return len(self._all[None]) == 0
        
        # better to assume that we're not empty on slower connections
        if orb.system.settings().optimizeDefaultEmpty():
            return False
        
        table = self.table()
        db = self.database()
        
        key = self.cacheKey(options)
        lookup       = self.lookupOptions(**options)
        db_opts      = self.databaseOptions(**options)
        
        if key in self._empty:
            return self._empty[key]
        
        options['limit'] = 1
        options['inflated'] = False
        
        empty = self.first(**options) == None
        self._empty[key] = empty
        return empty
    
    def isGrouped(self):
        """
        Returns whether or not this record set is intended to be grouped.  This
        method is used to share the intended default usage.  This does not force
        a record set to be grouped or not.
        
        :return     <bool>
        """
        return self._grouped
    
    def isInflated(self):
        """
        Returns whether or not this record set will be inflated.
        
        :return     <bool>
        """
        if ( self._inflated == None ):
            return self._columns is None
        return self._inflated
    
    def isLoaded(self):
        """
        Returns whether or not this record set already is loaded or not.
        
        :return     <bool>
        """
        return len(self._all) != 0
    
    def isOrdered(self):
        """
        Returns whether or not this record set is intended to be ordered.  This
        method is used to share the intended default usage.  This does not force
        a record set to be grouped or not.
        
        :return     <bool>
        """
        return self._ordered
    
    def isNull(self):
        """
        Returns whether or not this record set can contain valid data.
        
        :return     <bool>
        """
        table = self.table()
        db = self.database()
        
        return not (table and db and db.backend())
    
    def isThreadEnabled(self):
        """
        Returns whether or not this record set is threadable based on its
        database backend.
        
        :return     <bool>
        """
        db = self.database()
        if ( db ):
            return db.isThreadEnabled()
        return False
    
    def join(self, records):
        """
        Joins together a list of records or another record set to this instance.
        
        :param      records | <RecordSet> || <list> || None
        
        :return     <bool>
        """
        if isinstance(records, RecordSet):
            self._all[None] = self.all() + records.all()
            return True
        
        elif type(records) in (list, tuple):
            self._all[None] = self.all() + records
            return True
        
        else:
            return False
    
    def limit(self):
        """
        Returns the limit for this record set.
        
        :return     <int>
        """
        return self._limit
    
    def lookupOptions(self, **options):
        """
        Returns the lookup options for this record set.
        
        :return     <orb.LookupOptions>
        """
        kwds = options.copy()
        
        if 'where' in kwds and self.query() is not None:
            kwds['where'] = self.query() & kwds['where']
        else:
            kwds.setdefault('where', self.query())
        
        # initialize the options with this record sets values
        kwds.setdefault('expand',   self._expand)
        kwds.setdefault('columns',  self.columns())
        kwds.setdefault('where',    self.query())
        kwds.setdefault('order',    self.order())
        kwds.setdefault('start',    self.start())
        kwds.setdefault('limit',    self.limit())
        kwds.setdefault('inflated', self.isInflated())
        kwds.setdefault('ignoreJoined', self.ignoreJoined())
        kwds.setdefault('ignoreAggregates', self.ignoreAggregates())
        
        return orb.LookupOptions(**kwds)
    
    def namespace(self):
        """
        Returns the namespace for this query.
        
        :return     <str> || None
        """
        return self._namespace
    
    def order(self):
        """
        Returns the ordering information for this record set.
        
        :return     [(<str> field, <str> asc|desc), ..] || None
        """
        return self._order

    def ordered(self, *order):
        """
        Returns a newly ordered record set based on the inputed ordering.

        :param      order | [(<str> column, <str> asc | desc), ..]

        :return     <orb.RecordSet>
        """
        out = RecordSet(self)
        out.setOrder(order)
        return out

    def pageCount(self, pageSize=None):
        """
        Returns the number of pages that this record set contains.  If no page
        size is specified, then the page size for this instance is used.
        
        :sa         setPageSize
        
        :param      pageSize | <int> || None
        
        :return     <int>
        """
        # if there is no page size, then there is only 1 page of data
        if pageSize is None:
            pageSize = self.pageSize()
        else:
            pageSize = max(0, pageSize)
        
        if not pageSize:
            return 1
        
        # determine the number of pages in this record set
        pageFraction = self.totalCount() / float(pageSize)
        pageCount    = int(pageFraction)
        
        # determine if there is a remainder of records
        remain = pageFraction % 1
        if remain:
            pageCount += 1
        
        return pageCount
    
    def page(self, pageno, pageSize=None):
        """
        Returns the records for the current page, or the specified page number.
        If a page size is not specified, then this record sets page size will
        be used.
        
        :param      pageno   | <int>
                    pageSize | <int>
        
        :return     <orb.RecordSet>
        """
        if pageSize is None:
            pageSize = self.pageSize()
        else:
            pageSize = max(0, pageSize)
        
        # for only 1 page of information, return all information
        if not pageSize:
            return RecordSet(self)
        
        # lookup the records for the given page
        start = pageSize * (pageno - 1)
        limit = pageSize
        
        # returns a new record set with this start and limit information
        output = RecordSet(self)
        output.setCurrentPage(pageno)
        output.setPageSize(pageSize)
        output.setStart(start)
        output.setLimit(limit)
        
        return output
    
    def paged(self, pageSize=None):
        """
        Returns a broken up set of this record set based on its paging
        information.
        
        :return     [<orb.RecordSet>, ..]
        """
        if pageSize is None:
            pageSize = self.pageSize()
        else:
            pageSize = max(0, pageSize)
        
        if not pageSize or self.isEmpty():
            return []
        
        count = self.pageCount(pageSize)
        pages = []
        for i in range(count):
            page = RecordSet(self)
            page.setStart(i*pageSize)
            page.setLimit(pageSize)
            pages.append(page)
        
        return pages
    
    def pages(self, pageSize=None):
        """
        Returns a range for all the pages in this record set.
        
        :return     [<int>, ..]
        """
        return range(1, self.pageCount(pageSize) + 1)
    
    def pageSize(self):
        """
        Returns the default page size for this record set.  This can be used
        with the paging mechanism as the default value.
        
        :return     <int>
        """
        return self._pageSize or orb.system.settings().defaultPageSize()
    
    def primaryKeys(self, **options):
        """
        Returns a list of keys for the records defined for this recordset.
        
        :return     [<variant>, ..]
        """
        if None in self._all:
            return [record.id() for record in self._all[None]]
        
        elif self.table():
            cols = self.table().schema().primaryColumns()
            cols = map(lambda x: x.fieldName(), cols)
            return self.values(cols, **options)
        
        return self.values(orb.system.settings().primaryField(), **options)
    
    def query(self):
        """
        Returns the query for this record set.
        
        :return     <Query> || <QueryCompound> || None
        """
        return self._query
    
    def recordAt(self, index, **options):
        """
        Returns the record at the given index and current query information.
        Additional options can include any options available for the 
        <orb.LookupOptions> or <orb.DatabaseOptions> classes that will be passed
        to this recordset backend.
        
        :return     [<orb.Table>, ..]
        """
        has_default = 'default' in options
        default = options.get('default')
        
        table = self.table()
        db = self.database()
        
        key = self.cacheKey(options)
        lookup  = self.lookupOptions(**options)
        db_opts = self.databaseOptions(**options)
        
        if self.isNull():
            if not has_default:
                raise IndexError, index
            else:
                return default
        
        # return the cached lookups
        if key in self._all:
            try:
                return self._all[key][index]
            except IndexError:
                if not has_default:
                    raise
                else:
                    return default
        else:
            # create the lookup information
            cache   = table.recordCache()
            
            # grab the database backend
            backend = db.backend()
            if not backend:
                if not has_default:
                    raise IndexError, index
                else:
                    return default
            
            lookup.start = index
            lookup.limit = 1
            
            # cache the result for this query
            results = None
            if cache is not None and orb.system.isCachingEnabled():
                results = cache.select(backend, table, lookup, db_opts)
            else:
                try:
                    results = backend.select(table, lookup, db_opts)
                except errors.OrbError, err:
                    if db_opts.throwErrors:
                        raise
                    else:
                        log.error('Backend error occurred.\n%s', err)
                        results = []
            
            if not results:
                if not has_default:
                    raise IndexError, index
                else:
                    return default
            
            if db_opts.inflateRecords:
                return self.inflateRecord(table, results[0])
            elif lookup.columns and not options.get('ignoreColumns'):
                if len(lookup.columns) == 1:
                    return results[0][lookup.columns[0]]
                else:
                    return [results[0][col] for col in lookup.columns[0]]
            else:
                return results[0]
    
    def refine(self, query):
        """
        Creates a subset of this record set with a joined query based on the 
        inputed search text.  The search will be applied to all columns that are
        marked as searchable.
        
        :sa         Column.setSearchable
        
        :param      search_text | <str>
        
        :return     <RecordSet>
        """
        rset = RecordSet(self)
        rset.setQuery(query & self._query)
        return rset
    
    def remove(self, **options):
        """
        Removes the records from this set based on the inputed removal mode.
        
        :note       As of version 0.6.0 on, this method accepts variable 
                    keyword arguments.  This is to support legacy code,
                    the preferred method to call is to pass in options =
                    <orb.DatabaseOptions>, however, you can supply any
                    members as keyword arguments and it will generate an
                    options instance for you.
        
        :return     <int>
        """
        if self.isNull() or self.isEmpty():
            return 0
        
        try:
            backend = self.database().backend()
        except AttributeError:
            return 0
        
        # include cascading records
        breakdown = self._schemaBreakdown()
        dbopts = self.databaseOptions(**options)
        
        count = 0
        for table, query in breakdown.items():
            lookup = orb.LookupOptions(where=query)
            try:
                count += backend.remove(table, lookup, dbopts)
                table.markTableCacheExpired()
            except errors.OrbError, err:
                if dbopts.throwErrors:
                    raise
                else:
                    log.error('Backend error ocurred.\n%s', err)
        
        return count

    def search(self,
               search_terms,
               mode=SearchMode.All,
               limit=None,
               useThesaurus=True):
        """
        Creates a subset of this record set with a joined query based on the 
        inputed search text.  The search will be applied to all columns that are
        marked as searchable.
        
        :sa         Column.setSearchable
        
        :param      search_terms | [<str>, ..] || <str>
        
        :return     <RecordSet>
        """
        if not self.table():
            return RecordSet()
        
        if not search_terms:
            return RecordSet(self)
        
        engine = self.table().schema().searchEngine()
        terms = engine.parse(search_terms)
        output = self.refine(terms.toQuery(self.table()))
        if limit is not None:
            output.setLimit(limit)
        return output
        
    def setColumns(self, columns):
        """
        Sets the columns that this record set should be querying the database
        for.
        
        :param      columns | [<str>, ..] || None
        """
        self._columns = columns

    def setCurrentPage(self, pageno):
        """
        Sets the current page number that this record set is on.

        :param      pageno | <int>
        """
        self._currentPage = pageno

    def setDatabase(self, database):
        """
        Sets the database instance for this record set.  If it is left blank,
        then the default orb database for the table class will be used.
        
        :param      database | <Database> || None
        """
        self._database = database
    
    def setGrouped(self, state):
        """
        Sets whether or not this record set is intended to be grouped.  This
        method is used to share the intended default usage.  This does not force
        a record set to be grouped or not.
        
        :return     state | <bool>
        """
        self._grouped = state
    
    def setGroupBy(self, groupBy):
        """
        Sets the group by information that this record set will use.
        
        :param      groupBy | [<str>, ..] || None
        """
        self._groupBy = groupBy
    
    def setDatabaseOptions(self, options):
        """
        Sets the database options for selectin to the inputed options.
        
        :param      options | <orb.DatabaseOptions>
        """
        self._namespace = options.namespace
        self._inflated = options.inflateRecords
        self._locale = options.locale

    def setIgnoreAggregates(self, state=True):
        """
        Sets whether or not aggregate columns as a whole should be ignored in
        the results.  They can be loaded later, but by default they can
        take a long time to load for groups of queries.
        
        :param      state | <bool>
        """
        self._ignoreAggregates = state

    def setIgnoreJoined(self, state=True):
        """
        Sets whether or not joined columns as a whole should be ignored in
        the results.  They can be loaded later, but by default they can
        take a long time to load for groups of queries.
        
        :param      state | <bool>
        """
        self._ignoreJoined = state

    def setInflated(self, state):
        """
        Sets whether or not by default the results from this record set should
        be inflated.
        
        :param      state | <bool> || None
        """
        self._inflated = state
    
    def setLimit(self, limit):
        """
        Sets the limit for this record set.
        
        :param      limit | <int>
        """
        self._limit = limit
    
    def setLookupOptions(self, lookup):
        """
        Sets the lookup options for this instance to the inputed lookup data.
        
        :param      lookup | <orb.LookupOptions>
        """
        self._expand            = lookup.expand
        self._query             = lookup.where
        self._columns           = lookup.columns
        self._order             = lookup.order
        self._ordered           = lookup.order != None
        self._limit             = lookup.limit
        self._start             = lookup.start
        self._ignoreJoined      = lookup.ignoreJoined
        self._ignoreAggregates  = lookup.ignoreAggregates
    
    def setNamespace(self, namespace):
        """
        Sets the namespace information for this recordset to the given namespace
        
        :param      namespace | <str>
        """
        self._namespace = namespace
    
    def setOrder(self, order):
        """
        Sets the field order that this record set will use.
        
        :param      order | [(<str> field, <str> asc|desc), ..] || None
        """
        self._order = order
        self.setOrdered(order is not None)
    
    def setOrdered(self, state):
        """
        Sets whether or not this record set is intended to be grouped.  This
        method is used to share the intended default usage.  This does not force
        a record set to be ordered or not.
        
        :param      state | <bool>
        """
        self._ordered = state
    
    def setPageSize(self, pageSize):
        """
        Sets the page size for this record set to the inputed page size.
        
        :param      pageSize | <int>
        """
        self._pageSize = pageSize
    
    def setQuery(self, query):
        """
        Sets the query that this record set will use.  This will also clear the
        cache information since it will alter what is being stored.
        
        :param      query | <Query> || <QueryCompound> || None
        """
        self._query = query
        self.clear()
    
    def setValues(self, **values):
        """
        Sets the values within this record set to the inputed value dictionary
        or keyword mapping.
        """
        for record in self.all():
            for key, value in values.items():
                record.setRecordValue(key, value)
    
    def setStart(self, index):
        """
        Sets the start index for this query.
        
        :param      index | <int>
        """
        self._start = index
    
    def sumOf(self, columnName):
        """
        Returns the sum of the values from the column name.
        
        :return     <int>
        """
        return sum(self.values(columnName))
    
    def sort(self, cmp=None, key=None, reverse=False):
        """
        Sorts the resulted all records by the inputed arguments.
        
        :param      *args | arguments
        """
        self._sort_cmp_callable = cmp
        self._sort_key_callable = key
        self._sort_reversed     = reverse
    
    def start(self):
        """
        Returns the start index for this query.
        
        :return     <int>
        """
        return self._start
    
    def table(self):
        """
        Returns the table class that this record set is associated with.
        
        :return     <subclass of orb.Table>
        """
        return self._table

    def totalCount(self):
        """
        Returns the total number of records in this result set vs. the default count which
        will factor in the page size information.

        :return     <int>
        """
        return self.count(start=0, limit=None)

    def values(self, columns, **options):
        """
        Returns either a list of values for all the records if the inputed arg
        is a column name, or a dictionary of columnName values for multiple
        records for all the records in this set.
        
        :param      columns | <str> || [<str>, ..]
        
        :return     [<variant>, ..] || {<str> column: [<variant>, ..], ..}
        """
        # update the list
        if isinstance(columns, basestring):
            columns = [nstr(columns)]
        
        if self.isNull():
            return []
        
        key = self.cacheKey(options)
        table = self.table()
        if not table:
            return []

        schema = table.schema()
        db = self.database()

        # ensure that we're using the field names when looking in the database
        orb_columns = []
        fields = []
        for col in columns:
            orb_col = schema.column(col) if not isinstance(col, orb.Column) else col
            if not orb_col:
                raise orb.errors.ColumnNotFound(schema.name(), col)

            orb_columns.append(orb_col)
            fields.append(orb_col.fieldName())

        # create the lookup options
        options['columns'] = fields
        lookup = self.lookupOptions(**options)
        db_opts = self.databaseOptions(**options)

        # lookup the data
        cache = table.recordCache()
        if key in self._all:
            records = self._all[key]
        elif cache:
            records = cache.select(db.backend(), table, lookup, db_opts)
        else:
            try:
                records = db.backend().select(table, lookup, db_opts)
            except errors.OrbError, err:
                if db_opts.throwErrors:
                    raise
                else:
                    log.error('Backend error occurred.\n%s', err)
                    records = []
        
        # parse the values from the cache
        output = {}
        locale = db_opts.locale
        
        for record in records:
            for orb_col in orb_columns:
                colname = orb_col.name()
                output.setdefault(colname, [])
                expand = bool(orb_col.isReference() and db_opts.inflateRecords)
                
                # retreive the value
                if orb.Table.recordcheck(record):
                    value = record.recordValue(colname, autoInflate=expand)
                else:
                    value = record.get(colname)
                
                # grab specific locale translation options
                if orb_col.isTranslatable() and type(value) == dict:
                    if locale != 'all':
                        value = value.get(locale, '')

                # expand a reference object if desired
                if expand and value is not None:
                    ref_model = orb_col.referenceModel()
                    if not ref_model:
                        output[colname].append(None)
                    elif not isinstance(value, ref_model):
                        output[colname].append(ref_model(value))
                    else:
                        output[colname].append(value)

                # de-expand an already loaded reference object if IDs are all that is wanted
                elif not expand and orb.Table.recordcheck(value):
                    output[colname].append(value.id())

                # return a standard item
                else:
                    output[colname].append(value)
        
        if len(output) == 1:
            return output[orb_columns[0].name()]
        elif output:
            return zip(*[output[orb_col.name()] for orb_col in orb_columns])
        else:
            return []
    
    @staticmethod
    def typecheck(value):
        """
        Checks to see if the inputed type is of a Recordset
        
        :param      value | <variant>
        
        :return     <bool>
        """
        return isinstance(value, RecordSet)