""" 
Creates a class for manipulating groups of records at once. 
This is useful when you need to remove a lot of records at one time, and is the
return result from the select mechanism that supports paging.
"""

# ------------------------------------------------------------------------------

import logging
import projex.iters
import projex.rest
import re

from collections import defaultdict
from projex.lazymodule import lazy_import
from xml.etree import ElementTree

from ..common import SearchMode

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


# ------------------------------------------------------------------------------

class RecordSetIterator(object):
    def __init__(self, rset, batch=1):
        self._rset = rset
        self._page = 1
        self._index = -1
        self._pageSize = batch
        self._records = []

    def __iter__(self):
        return self

    def next(self):
        self._index += 1

        # get the next batch of records
        if len(self._records) <= self._index:
            self._records = list(self._rset.page(self._page, self._pageSize))
            self._page += 1
            self._index = 0

        # stop the iteration when complete
        if not self._records:
            raise StopIteration()
        else:
            return self._records[self._index]

class RecordSet(object):
    """
    Defines a class that can be used to manipulate table records in bulk.  For
    more documentation on the RecordSet, check out the 
    [[$ROOT/recordsets|record set documentation]].
    """

    def __json__(self, *args):
        """
        Returns this record set as a list of records.

        :return     [<orb.Record>, ..]
        """
        return self.json()

    def __init__(self, *args, **kwds):
        self._table = None
        self._sourceColumn = kwds.get('sourceColumn')
        self._source = kwds.get('source')

        # default options
        self._cache = defaultdict(dict)
        self._grouped = False

        # sorting options
        self._sort_cmp_callable = None
        self._sort_key_callable = None
        self._sort_reversed = False

        # select information
        self._database = -1
        self._groupBy = None
        self._lookupOptions = orb.LookupOptions(**kwds)
        self._contextOptions = orb.ContextOptions(**kwds)

        # join another record set as RecordSet(recordSet)
        if args:
            data = args[0]

            if RecordSet.typecheck(data):
                self.duplicate(data)

            # join a list of records as RecordSet([record, record, record])
            elif type(data) in (list, tuple):
                self._cache['records'][None] = data[:]
                if data and (orb.Table.recordcheck(data[0]) or orb.View.recordcheck(data[0])):
                    self._table = type(data[0])

            # assign a table as the record set RecordSet(orb.Table) (blank record set for a given table)
            elif orb.Table.typecheck(data) or orb.View.typecheck(data):
                self._table = data

                # define a blank recordset
                if len(args) == 1:
                    self._cache['records'][None] = []

                # define a cache for this recordset
                elif type(args[1]) in (list, tuple):
                    self._cache['records'][None] = args[1][:]

                # define a recordset that should lookup in the future
                elif args[1] is None:
                    pass

    def __len__(self):
        """
        The length of this item will be its total count.
        
        :return     <int>
        """
        # return the length of the all records cache
        if None in self._cache['records']:
            return len(self._cache['records'][None])

        # return 0 for null record sets
        return 0 if self.isNull() else self.count()

    def __iter__(self):
        """
        Return the fully resolved list of data.
        
        :return     <list>
        """
        records = self.records()
        for record in records:
            yield record

    def __add__(self, other):
        """
        Adds two record set instances together and returns them.
        
        :param      other | <RecordSet> || <list>
        """
        return self.union(other)

    def __sub__(self, other):
        return self.difference(other)

    def __getitem__(self, value):
        """
        Returns the item at the inputted index for this record set.
        
        :return     <orb.Table>
        """
        # slice up this record set based on the inputted information
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
            if None in rset._cache['records']:
                rset._cache['records'][None] = rset._cache['records'][None][value]

            return rset

        # return the record from a given index
        else:
            return self.recordAt(value)

    def __nonzero__(self):
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
        if not options and None in self._cache['records']:
            output = {}
            for record in self._cache['records'][None]:
                table = type(record)
                output.setdefault(table, set())
                output[table].add(record.primaryKey())

            return {table: orb.Query(table).in_(keys)
                    for table, keys in output.items()}

        else:
            lookup = self.lookupOptions(**options)
            return {self.table(): lookup.where}

    def records(self, **options):
        """
        Looks up all the records based on the current query information.
        Additional options can include any options available for the 
        <orb.LookupOptions> or <orb.ContextOptions> classes that will be passed
        to this recordset backend.
        
        :return     [<orb.Table>, ..]
        """
        key = self.cacheKey(options)

        # return the cached lookups
        try:
            return self._cache['records'][key]
        except KeyError:
            if self.isNull():
                return []

            table = self.table()
            db = self.database()

            lookup = self.lookupOptions(**options)
            ctxt_opts = self.contextOptions(**options)

            # create the lookup information
            cache = table.recordCache()

            # grab the database backend
            backend = db.backend()
            if not backend:
                return []

            # cache the result for this query
            if cache is not None and orb.system.isCachingEnabled():
                results = cache.select(backend, table, lookup, ctxt_opts)
            else:
                results = backend.select(table, lookup, ctxt_opts)

            # return specific columns
            if ctxt_opts.inflated and lookup.returning != 'values':
                output = [self.inflateRecord(table, x) for x in results]

            elif lookup.columns:
                if lookup.returning == 'values':
                    cols = lookup.schemaColumns(self.table().schema())
                    if len(cols) == 1:
                        col = cols[0]
                        output = [x[col.fieldName()] for x in results]
                    else:
                        output = [[r.get(col.fieldName(), None) for col in cols]
                                  for r in results]
                else:
                    cols = lookup.schemaColumns(self.table().schema())
                    output = [{col.fieldName(): r.get(col.fieldName(), None) for col in cols} for r in results]

            # return the raw results
            else:
                output = results

            self._cache['records'][key] = output

        # return sorted results from an in-place sort
        if self._sort_cmp_callable is not None or \
           self._sort_key_callable is not None:
            return sorted(output,
                          cmp=self._sort_cmp_callable,
                          key=self._sort_key_callable,
                          reverse=self._sort_reversed)
        return output

    def cache(self, section, data, **options):
        """
        Caches the given section and data value for this record set.  The cache key will be determined
        based on the inputted option set.

        :param      section | <str> | records, count, first or last
                    data    | <variant>
                    **options
        """
        key = self.cacheKey(options)
        if data is not None:
            self._cache[section][key] = data
        else:
            self._cache[section].pop(key, None)

    def cacheKey(self, options):
        """
        Returns the cache key for based on the inputted dictionary of
        options.
        
        :param      options | <dict>
        
        :return     <hash>
        """
        options = {k: v for k, v in options.items() if k != 'expand'}

        if not options:
            return None
        else:
            return hash(self.lookupOptions(**options))

    def clear(self):
        """
        Clears all the cached information such as the all() records,
        records, and total numbers.
        """
        self._cache.clear()

    def commit(self, **options):
        """
        Commits the records associated with this record set to the database.
        """
        db = options.pop('db', self.database())
        backend = db.backend()

        lookup = self.lookupOptions(**options)
        db_options = self.contextOptions(**options)
        records = self.records(**options)

        # run each records pre-commit logic before it is inserted to the db
        for record in records:
            record.callbacks().emit('aboutToCommit(Record,LookupOptions,ContextOptions)', record, lookup, db_options)

        inserts = filter(lambda x: not x.isRecord(db=db), records)
        updates = filter(lambda x: x.isRecord(db=db), records)

        if inserts:
            backend.insert(inserts, lookup, db_options)
        if updates:
            backend.update(updates, lookup, db_options)

        # update the caching table information
        for table in set([type(record) for record in records]):
            table.markTableCacheExpired()

        # run each records pre-commit logic before it is inserted to the db
        for record in records:
            record.callbacks().emit('commitFinished(Record,LookupOptions,ContextOptions)', record, lookup, options)

        return True

    def count(self, **options):
        """
        Collects the count of records for this record set.  Additional options
        can include any options available for the <orb.LookupOptions> or 
        <orb.ContextOptions> classes that will be passed to this records
        backend.
        
        :return     <int>
        """
        table = self.table()
        db = self.database()

        key = self.cacheKey(options)
        try:
            return self._cache['count'][key]
        except KeyError:
            try:
                return len(self._cache['records'][key])
            except KeyError:
                if self.isNull():
                    return 0

                lookup = self.lookupOptions(**options)
                ctxt_opts = self.contextOptions(**options)

                # don't lookup unnecessary data
                lookup.columns = table.schema().primaryColumns()
                lookup.expand = None  # no need to include anything fancy
                lookup.order = None   # it does not matter the order for counting purposes

                # retrieve the count information
                cache = table.recordCache()
                if cache is not None and orb.system.isCachingEnabled():
                    count = cache.count(db.backend(), table, lookup, ctxt_opts)
                else:
                    count = db.backend().count(table, lookup, ctxt_opts)

                self._cache['count'][key] = count
                return count

    def columns(self):
        """
        Returns the columns that this record set should be querying for.
        
        :return     [<str>, ..] || None
        """
        return self._lookupOptions.columns

    def createRecord(self, values, **options):
        """
        Creates a new record for this recordset.  If this set was generated from a reverse lookup, then a pointer
        back to the source record will automatically be generated.

        :param      values | <dict>

        :return     <orb.Table>
        """
        if self.sourceColumn():
            values.setdefault(self.sourceColumn(), self.source())

        if isinstance(self.table(), orb.View):
            raise errors.ActionNotAllowed('View tables are read-only.')

        return self.table().createRecord(values, **options)

    def currentPage(self):
        """
        Returns the current page that this record set represents.

        :return     <int>
        """
        return self._lookupOptions.page

    def database(self):
        """
        Returns the database instance that this recordset will use.
        
        :return     <Database>
        """
        if self._database != -1:
            return self._database

        db = self.table().getDatabase() if self.table() else orb.system.database()
        if not db:
            raise errors.DatabaseNotFound()

        self._database = db
        return db

    def contextOptions(self, **options):
        """
        Returns the database options for this record set.  See the
        <orb.ContextOptions> documentation about the optional arguments.
        
        :return     <orb.ContextOptions>
        """
        opts = orb.ContextOptions(options=self._contextOptions)
        opts.update(options)
        return opts

    def difference(self, records):
        """
        Joins together a list of records or another record set to this instance.

        :param      records | <RecordSet> || <list> || None

        :return     <bool>
        """
        out = RecordSet(self)
        if isinstance(records, RecordSet):
            out._lookupOptions.where = records.lookupOptions().where.negated() & out._lookupOptions.where

        elif orb.Query.typecheck(records):
            out._lookupOptions.where = records.negated() & out._lookupOptions.where

        elif type(records) in (list, tuple):
            out._cache['records'][None] = self.records() + records

        else:
            raise TypeError(records)

        return out

    # noinspection PyProtectedMember
    def duplicate(self, other):
        """
        Duplicates the data from the other record set instance.
        
        :param      other | <RecordSet>
        """
        # default options
        self._grouped = other._grouped
        self._cache.update({k: v.copy() for k, v in other._cache.items()})

        # sorting options
        self._sort_cmp_callable = other._sort_cmp_callable
        self._sort_key_callable = other._sort_key_callable
        self._sort_reversed = other._sort_reversed

        # select information
        self._database = other._database
        self._groupBy = list(other._groupBy) if other._groupBy else None
        self._contextOptions = other.contextOptions().copy()
        self._lookupOptions = other.lookupOptions().copy()
        self._table = other._table

    def distinct(self, columns, **options):
        """
        Returns a distinct series of column values for the current record set
        based on the inputted column list.
        
        The additional options are any keyword arguments supported by the
        <orb.LookupOptions> or <orb.ContextOptions> classes.
        
        If there is one column supplied, then the result will be a list, 
        otherwise, the result will be a dictionary.
        
        :param      column   | <str> || [<str>, ..]
                    **options
        
        :return     [<variant>, ..] || {<str> column: [<variant>, ..]}
        """
        # ensure we have a database and table class
        if self.isNull():
            return {} if len(columns) > 1 else []

        table = self.table()
        schema = table.schema()

        # ensure we have a list of values
        if not type(columns) in (list, tuple):
            columns = [schema.column(columns)]
        else:
            columns = [schema.column(col) for col in columns]

        db = self.database()

        # return information from the database
        cache = table.recordCache()
        schema = table.schema()

        results = {}
        if options.get('inflated', True):
            for column in columns:
                if column.isReference():
                    ref_model = column.referenceModel()
                    lookup = self.lookupOptions(**options)
                    lookup.columns = [column]
                    lookup.distinct = True
                    lookup.order = None
                    rset = orb.RecordSet(self)
                    rset.setLookupOptions(lookup)
                    rset.setContextOptions(self.contextOptions(**options))
                    results[column] = ref_model.select(where=orb.Query(ref_model).in_(rset))

            lookup_columns = list(set(columns) - set(results.keys()))
        else:
            lookup_columns = list(columns)

        # perform an actual lookup
        if lookup_columns:
            ctxt_opts = self.contextOptions(**options)
            lookup = self.lookupOptions(**options)
            lookup.columns = lookup_columns
            backend = db.backend()
            if cache is not None and orb.system.isCachingEnabled():
                output = cache.distinct(backend, table, lookup, ctxt_opts)
            else:
                output = backend.distinct(table, lookup, ctxt_opts)
        else:
            output = {}

        results.update({schema.column(k): v for k, v in output.items()})
        return results.get(columns[0], []) if len(columns) == 1 else results

    def first(self, **options):
        """
        Returns the first record that matches the current query.
        
        :return     <orb.Table> || None
        """
        table = self.table()
        db = self.database()

        key = self.cacheKey(options)
        try:
            out = self._cache['first'][key]
            out.updateOptions(**options)
            return out
        except KeyError:
            try:
                return self._cache['records'][key][0]
            except IndexError:
                return None
            except KeyError:
                options['limit'] = 1
                lookup = self.lookupOptions(**options)
                ctxt_opts = self.contextOptions(**options)

                if self.isNull():
                    return None

                lookup.order = lookup.order or [(table.schema().primaryColumns()[0].name(), 'asc')]

                # retrieve the data from the cache
                cache = table.recordCache()
                if cache is not None and orb.system.isCachingEnabled():
                    records = cache.select(db.backend(), table, lookup, ctxt_opts)
                else:
                    records = db.backend().select(table, lookup, ctxt_opts)

                if records:
                    if ctxt_opts.inflated and lookup.returning!= 'values':
                        record = self.inflateRecord(table, records[0], **options)
                        record.setLookupOptions(lookup)
                        record.setContextOptions(ctxt_opts)
                    else:
                        record = records[0]
                else:
                    record = None

                self._cache['first'][key] = record
                return record

    def filter(self, **options):
        """
        Shortcut for refining this object set based on a dictionary based matching.  This is the same as doing
        `self.refine(Q.build(options))`.

        :param      **options | key/value pairing of options to filter by

        :return     <orb.RecordSet>
        """
        if not options:
            return self
        else:
            return self.refine(orb.Query.build(options))

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

        table = self.table()
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
                lookup = self.lookupOptions(**options)
                db_options = self.contextOptions(**options)

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
                    sub_set = RecordSet(table, None)
                    lookup = self.lookupOptions(**options)
                    db_options = self.contextOptions(**options)

                    # join the lookup options
                    if lookup.where is None:
                        lookup.where = sub_query
                    else:
                        lookup.where &= sub_query

                    sub_set.setLookupOptions(lookup)
                    sub_set.setContextOptions(db_options)

                    if remain:
                        sub_set = sub_set.grouped(remain, **options)

                    output[key] = sub_set

        return output

    def hasRecord(self, record, **options):
        """
        Returns whether or not this record set contains the inputted record.

        :param      record | <orb.Table>

        :return     <bool>
        """
        try:
            id_ = record.id()
            if type(record) != self.table():
                return False
        except AttributeError:
            try:
                id_ = record['id']
            except KeyError:
                id_ = record

        return id_ in self.ids()

    def ids(self, **options):
        """
        Returns a list of the ids that are associated with this record set.

        :sa         primaryKeys

        :return     [<variant>, ..]
        """
        try:
            return [record.id() if isinstance(record, orb.Table) else record for record in self._cache['ids'][None]]
        except KeyError:
            pass

        if self.table():
            cols = self.table().schema().primaryColumns()
            cols = [col.fieldName() for col in cols]
            return self.values(cols, **options)

        return self.values(orb.system.settings().primaryField(), **options)

    def inflateRecord(self, table, record, **options):
        """
        Inflates the record for the given class, applying a namespace override
        if this record set is using a particular namespace.
        
        :param      table       | <subclass of orb.Table>
                    record      | <dict>

        :return     <orb.Table>
        """
        inst = table.inflateRecord(record, options=self.contextOptions(**options))
        inst.setLookupOptions(self.lookupOptions(**options))
        return inst

    def index(self, record):
        """
        Returns the index of the inputted record within the all list.
        
        :param      record | <orb.Table>
        
        :return     <int>
        """
        if not record:
            return -1
        else:
            try:
                return self._cache['records'][None].index(record)
            except KeyError:
                return self.ids().index(record.id())

    def indexed(self, columns=None, **options):
        """
        Returns the records in a particular grouping.  If the groupBy option
        is left as None, then the base grouping for this instance will be used.
        
        :param      columns | <str> columnName || [<str> columnName, ..]
        
        :return     { <variant> grouping: <orb.Table>, .. }
        """
        if columns is None:
            return dict([(x.primaryKey(), x) for x in self.records(**options)])

        if not type(columns) in (list, tuple):
            columns = [columns]

        table = self.table()
        output = {}

        indexInflated = options.pop('indexInflated', True)

        if columns:
            for record in self.records(**options):
                key = []
                for column in columns:
                    key.append(record.recordValue(column, inflated=indexInflated))

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
        ctxt_opts = orb.ContextOptions(**options)
        ctxt_opts.force = True

        backend = db.backend()
        records = self.records()
        backend.insert(records, lookup, ctxt_opts)
        return True

    def isEmpty(self, **options):
        """
        Returns whether or not this record set contains any records.
        
        :return     <bool>
        """
        if self.isNull():
            return True

        try:
            return len(self._cache['records'][None]) == 0
        except KeyError:
            pass

        # better to assume that we're not empty on slower connections
        if orb.system.settings().optimizeDefaultEmpty():
            return False

        key = self.cacheKey(options)
        try:
            return self._cache['empty'][key]
        except KeyError:
            try:
                return len(self._cache['records'][key]) == 0
            except KeyError:
                # define custom options
                options['columns'] = ['id']
                options['limit'] = 1
                options['inflated'] = False

                is_empty = self.first(**options) is None
                self._cache['empty'][key] = is_empty
                return is_empty

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
        return self._lookupOptions.returning != 'values' and self._contextOptions.inflated

    def isLoaded(self):
        """
        Returns whether or not this record set already is loaded or not.
        
        :return     <bool>
        """
        return len(self._cache['records']) != 0

    def isOrdered(self):
        """
        Returns whether or not this record set is intended to be ordered.  This
        method is used to share the intended default usage.  This does not force
        a record set to be grouped or not.
        
        :return     <bool>
        """
        return self._lookupOptions.order is not None

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
        if db:
            return db.isThreadEnabled()
        return False

    def iterrecords(self, batch=1):
        """
        Creates a recordset iterator that will loop through the contents of this
        record set at a particular size.  This will reduce the number of calls made to the
        database at a single time.

        :param batch:int

        :return: <orb.data.recordset.RecordSetIterator
        """
        return RecordSetIterator(self, batch)

    def update(self, **values):
        """
        Updates the records within this set based on the inputted values.

        :param      **values | <dict>
        """
        raise errors.ActionNotAllowed('Bulk editing of records is not allowed.')

    def updateOptions(self, **options):
        self._lookupOptions.update(options)
        self._contextOptions.update(options)

    def union(self, records):
        """
        Joins together a list of records or another record set to this instance.
        
        :param      records | <RecordSet> || <list> || None
        
        :return     <bool>
        """
        out = RecordSet(self)
        if isinstance(records, RecordSet):
            out._lookupOptions.where = records.lookupOptions().where | out._lookupOptions.where

        elif orb.Query.typecheck(records):
            out._lookupOptions.where = records | out._lookupOptions.where

        elif type(records) in (list, tuple):
            out._cache['records'][None] = self.records() + records

        else:
            raise TypeError(records)

        return out

    def json(self, **options):
        """
        Returns the records for this set as a json string.

        :return     <str>
        """
        lookup = self.lookupOptions(**options)
        context = self.contextOptions(**options)

        # lookup specific return types
        if lookup.returning == 'count':
            return len(self)
        elif lookup.returning == 'first':
            return self.first().json()
        elif lookup.returning == 'last':
            return self.last().json()

        tree = lookup.expandtree()
        output = {}
        if 'first' in tree:
            options['expand'] = tree.pop('first')
            output['first'] = self.first(**options)
        if 'last' in tree:
            options['expand'] = tree.pop('last')
            output['last'] = self.last(**options)
        if 'ids' in tree:
            tree.pop('ids')
            output['ids'] = self.ids(**options)
        if 'count' in tree:
            tree.pop('count')
            output['count'] = self.count(**options)
        if 'records' in tree:
            sub_tree = tree.pop('records', tree)
            sub_tree.update(tree)
            options['expand'] = sub_tree

            if lookup.returning != 'values':
                output['records'] = [record.json(lookup=lookup, options=context) for record in self.records()]
            else:
                output['records'] = self.records()

        if not output:
            if lookup.returning != 'values':
                output = [record.json(lookup=lookup, options=context) for record in self.records()]
            else:
                output = self.records()

        if context.format == 'text':
            return projex.rest.jsonify(output)
        else:
            return output

    def last(self, **options):
        """
        Returns the last record for this set by inverting the order of the lookup.  If
        no order has been defined, then the primary column will be used as the ordering.

        :return     <orb.Table> || None
        """
        key = self.cacheKey(options)
        try:
            out = self._cache['last'][key]
            try:
                updater = out.updateOptions
            except AttributeError:
                pass
            else:
                updater(**options)
            return out
        except KeyError:
            try:
                return self._cache['records'][key][-1]
            except KeyError:
                if self.isNull():
                    return None

                record = self.reversed().first(**options)
                self._cache['last'][key] = record
                return record

    def limit(self):
        """
        Returns the limit for this record set.
        
        :return     <int>
        """
        return self._lookupOptions.limit

    def lookupOptions(self, **options):
        """
        Returns the lookup options for this record set.
        
        :return     <orb.LookupOptions>
        """
        lookup = orb.LookupOptions(lookup=self._lookupOptions)
        lookup.update(options)
        return lookup

    def namespace(self):
        """
        Returns the namespace for this query.
        
        :return     <str> || None
        """
        return self._contextOptions.namespace

    def order(self):
        """
        Returns the ordering information for this record set.
        
        :return     [(<str> field, <str> asc|desc), ..] || None
        """
        return self._lookupOptions.order

    def ordered(self, *order):
        """
        Returns a newly ordered record set based on the inputted ordering.

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
        pageCount = int(pageFraction)

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
            page.setStart(i * pageSize)
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
        return self._lookupOptions.pageSize or orb.system.settings().defaultPageSize()

    def primaryKeys(self, **options):
        """
        Returns a list of keys for the records defined for this recordset.
        
        :return     [<variant>, ..]
        """
        return self.ids(**options)

    def query(self):
        """
        Returns the query for this record set.
        
        :return     <Query> || <QueryCompound> || None
        """
        return self._lookupOptions.where

    def recordAt(self, index, **options):
        """
        Returns the record at the given index and current query information.
        Additional options can include any options available for the 
        <orb.LookupOptions> or <orb.ContextOptions> classes that will be passed
        to this recordset backend.
        
        :return     [<orb.Table>, ..]
        """
        has_default = 'default' in options
        default = options.pop('default', None)

        key = self.cacheKey(options)
        try:
            return self._cache['records'][key][index]
        except KeyError:
            if self.isNull():
                if has_default:
                    return default
                else:
                    raise IndexError(index)

            record = self.first(**options)
            if record is not None:
                return record
            elif has_default:
                return default
            else:
                raise IndexError(index)

    def refine(self, *args, **options):
        """
        Creates a subset of this record set with a joined query based on the 
        inputted search text.  The search will be applied to all columns that are
        marked as searchable.

        :return     <RecordSet>
        """
        rset = RecordSet(self)

        # backward compatibility support
        if args and orb.Query.typecheck(args[0]):
            options.setdefault('where', args[0])

        rset.setLookupOptions(self.lookupOptions(**options))
        rset.setContextOptions(self.contextOptions(**options))

        if 'terms' in options and options['terms']:
            return rset.search(options['terms'])
        else:
            return rset

    def remove(self, **options):
        """
        Removes the records from this set based on the inputted removal mode.
        
        :note       As of version 0.6.0 on, this method accepts variable 
                    keyword arguments.  This is to support legacy code,
                    the preferred method to call is to pass in options =
                    <orb.ContextOptions>, however, you can supply any
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
        dbopts = self.contextOptions(**options)

        count = 0
        for table, query in breakdown.items():
            lookup = orb.LookupOptions(where=query)
            count += backend.remove(table, lookup, dbopts)
            table.markTableCacheExpired()

        return count

    def reversed(self, **options):
        """
        Returns a recordset with the reversed order from this set.

        :return     <orb.RecordSet>
        """
        rset = RecordSet(self)
        order = options.get('order') or self.order() or [(self.table().schema().primaryColumns()[0], 'asc')]
        order = [(col, 'asc' if direct == 'desc' else 'desc') for col, direct in order]
        rset.setOrder(order)
        return rset

    def search(self,
               search_terms,
               mode=SearchMode.All,
               limit=None,
               useThesaurus=True):
        """
        Creates a subset of this record set with a joined query based on the 
        inputted search text.  The search will be applied to all columns that are
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
        output = self.refine(engine.parseQuery(self.table(), search_terms))
        if limit is not None:
            output.setLimit(limit)
        return output

    def setColumns(self, columns):
        """
        Sets the columns that this record set should be querying the database
        for.
        
        :param      columns | [<str>, ..] || None
        """
        self._lookupOptions.columns = columns

    def setCurrentPage(self, pageno):
        """
        Sets the current page number that this record set is on.

        :param      pageno | <int>
        """
        self._lookupOptions.page = pageno

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

    def setContextOptions(self, options):
        """
        Sets the database options for selectin to the inputted options.
        
        :param      options | <orb.ContextOptions>
        """
        self._contextOptions = options.copy()

    def setInflated(self, state):
        """
        Sets whether or not by default the results from this record set should
        be inflated.
        
        :param      state | <bool> || None
        """
        self._contextOptions.inflated = state

    def setLimit(self, limit):
        """
        Sets the limit for this record set.
        
        :param      limit | <int>
        """
        self._lookupOptions.limit = limit

    def setLookupOptions(self, lookup):
        """
        Sets the lookup options for this instance to the inputted lookup data.
        
        :param      lookup | <orb.LookupOptions>
        """
        self._lookupOptions = lookup.copy()

    def setNamespace(self, namespace):
        """
        Sets the namespace information for this recordset to the given namespace
        
        :param      namespace | <str>
        """
        self._contextOptions.namespace = namespace

    def setOrder(self, order):
        """
        Sets the field order that this record set will use.
        
        :param      order | [(<str> field, <str> asc|desc), ..] || None
        """
        self._lookupOptions.order = order

    def setPageSize(self, pageSize):
        """
        Sets the page size for this record set to the inputted page size.
        
        :param      pageSize | <int>
        """
        self._lookupOptions.pageSize = pageSize

    def setQuery(self, query):
        """
        Sets the query that this record set will use.  This will also clear the
        cache information since it will alter what is being stored.
        
        :param      query | <Query> || <QueryCompound> || None
        """
        self._lookupOptions.where = query
        self.clear()

    def setValues(self, **values):
        """
        Sets the values within this record set to the inputted value dictionary
        or keyword mapping.
        """
        for record in self.records():
            for key, value in values.items():
                record.setRecordValue(key, value)

    def setSourceColumn(self, column):
        """
        Sets the column that was used to call and generate this recordset.  This method is
        used within the reverselookupmethod and is not needed to be called directly.

        :param      column | <str>
        """
        self._sourceColumn = column

    def setSource(self, source):
        """
        Sets the record that was used to call and generate this recordset.  This method is
        used within the reverselookupmethod and is not needed to be called directly.

        :param      source | <orb.Table> || None
        """
        self._source = source

    def setStart(self, index):
        """
        Sets the start index for this query.
        
        :param      index | <int>
        """
        self._lookupOptions.start = index

    def sumOf(self, columnName):
        """
        Returns the sum of the values from the column name.
        
        :return     <int>
        """
        return sum(self.values(columnName))

    def sourceColumn(self):
        """
        When used in a reverse lookup, the source column represents the column from the record that created
        this recordset lookup.

        :return     <str> || None
        """
        return self._sourceColumn

    def source(self):
        """
        When used in a reverse lookup, the source is a pointer to the record that generated this recordset
        lookup.

        :return     <orb.Table> || None
        """
        return self._source

    # noinspection PyShadowingBuiltins
    def sort(self, cmp=None, key=None, reverse=False):
        """
        Sorts the resulted all records by the inputted arguments.
        
        :param      *args | arguments
        """
        self._sort_cmp_callable = cmp
        self._sort_key_callable = key
        self._sort_reversed = reverse

    def start(self):
        """
        Returns the start index for this query.
        
        :return     <int>
        """
        return self._lookupOptions.start

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
        return self.count(start=0, limit=0, page=0, pageSize=0)

    def toXml(self, xparent=None):
        """
        Converts this recordset to a representation of XML.

        :param      xparent | <ElementTree.Element> || None

        :return     <ElementTree.Element>
        """
        xset = ElementTree.SubElement(xparent, 'recordset') if xparent is not None else ElementTree.Element('recordset')
        xset.set('table', self.table().schema().name())
        xlookup = ElementTree.SubElement(xset, 'lookup')
        xoptions = ElementTree.SubElement(xset, 'options')
        self._lookupOptions.toXml(xlookup)
        self._contextOptions.toXml(xoptions)
        return xset

    def values(self, columns, **options):
        """
        Returns either a list of values for all the records if the inputted arg
        is a column name, or a dictionary of columnName values for multiple
        records for all the records in this set.
        
        :param      columns | <str> || [<str>, ..]
        
        :return     [<variant>, ..] || {<str> column: [<variant>, ..], ..}
        """
        if self.isNull() or self.table() is None:
            return []

        schema = self.table().schema()
        if type(columns) not in (list, set, tuple):
            columns = [schema.column(columns)]
        else:
            columns = [schema.column(col) for col in columns]

        key = self.cacheKey(options)
        db = self.database()

        # create the lookup options
        options['columns'] = columns[:]
        lookup = self.lookupOptions(**options)
        ctxt_opts = self.contextOptions(**options)

        # lookup the data
        cache = self.table().recordCache()
        if key in self._cache['records']:
            records = self._cache['records'][key]
        elif cache:
            records = cache.select(db.backend(), self.table(), lookup, ctxt_opts)
        else:
            records = db.backend().select(self.table(), lookup, ctxt_opts)

        # parse the values from the cache
        output = defaultdict(list)
        locale = ctxt_opts.locale

        inflated = ctxt_opts.inflated and lookup.returning != 'values'

        for record in records:
            for column in columns:
                expand = bool(column.isReference() and inflated)

                # retrieve the value
                if orb.Table.recordcheck(record) or orb.View.recordcheck(record):
                    value = record.recordValue(column, inflated=expand)
                else:
                    value = record.get(column.fieldName())

                # grab specific locale translation options
                if column.isTranslatable() and type(value) == dict:
                    if locale != 'all':
                        value = value.get(locale, '')

                # expand a reference object if desired
                if expand and value is not None:
                    ref_model = column.referenceModel()
                    if not ref_model:
                        output[column].append(None)
                    elif not isinstance(value, ref_model):
                        output[column].append(ref_model(value))
                    else:
                        output[column].append(value)

                # de-expand an already loaded reference object if IDs are all that is wanted
                elif not expand and (orb.Table.recordcheck(value) or orb.View.recordcheck(value)):
                    output[column].append(value.id())

                # return a standard item
                else:
                    output[column].append(value)

        if len(output) == 1:
            return output[columns[0]]
        elif output:
            return zip(*[output[column] for column in columns])
        else:
            return []

    def view(self, name):
        """
        Returns a set of records represented as a view that matches the inputted name.

        :param      name | <str>

        :return     <orb.RecordSet> || None
        """
        table = self.table()
        if not table:
            return None

        view = table.schema().view(name)
        if view:
            if not self.isEmpty():
                return view.select(where=orb.Query(view).in_(self), options=self.contextOptions())
            else:
                return orb.RecordSet(view)
        else:
            return None

    @staticmethod
    def typecheck(value):
        """
        Checks to see if the inputted type is of a Recordset
        
        :param      value | <variant>
        
        :return     <bool>
        """
        return isinstance(value, RecordSet)

    @staticmethod
    def fromXml(xset):
        """
        Restores a recordset from XML.

        :param      xset | <ElementTree.Element> || None

        :return     <orb.RecordSet>
        """
        model = orb.system.model(xset.get('table'))
        lookup = orb.LookupOptions.fromXml(xset.find('lookup'))
        options = orb.ContextOptions.fromXml(xset.find('options'))
        return model.select(lookup=lookup, options=options)