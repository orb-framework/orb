import demandimport
import logging
import math

from collections import defaultdict
from ..decorators import deprecated
from ..utils.locks import ReadWriteLock, ReadLocker, WriteLocker

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class BatchIterator(object):
    """
    Iterates over a collection in batches by stringing together pages
    of queries.

    :usage

        from orb import Collection, BatchIterator

        collection = Collection(range(100))
        iter = BatchIterator(collection, batch=10)

        for record in iter:
            # do something

    This method can also be invoked from the collection object itself via
    the iterate helper method.

    :usage

        from orb import Collection

        collection = Collection(range(100))
        for record in collection.iterate(batch=10):
            # do something
    """
    def __init__(self, collection, batch=1):
        self.collection = collection
        self.batch_size = batch

    def __iter__(self):
        page_count = self.collection.page_count(page_size=self.batch_size)
        for page_number in xrange(page_count):
            page = self.collection.page(page_number + 1, page_size=self.batch_size)
            for record in page:
                yield record


class Collection(object):
    """
    Defines an object class that represents a collection of model records.
    """
    def __init__(self, records=None, model=None, **context):
        # define custom properties
        self.__lock = ReadWriteLock()
        self.__cache = defaultdict(dict)
        self.__context = orb.Context(**context)

        self.__bound_model = model
        self.__bound_collector = None
        self.__bound_source_record = None

        # store the records for this collection if provided
        if records is not None:
            # update the bound model if specified
            if self.__bound_model is None and len(records) > 0:
                record_type = type(records[0])
                if issubclass(record_type, orb.Model):
                    self.__bound_model = record_type

            # cache the records by default
            self.__cache['records'][self.__context] = records

    def __bool__(self):  # pragma: no cover
        return not self.is_empty()

    def __json__(self):
        """
        Serializes this object to JSON acceptable dictionary values.

        :return: <dict>
        """
        context = self.context()
        expand = context.expandtree(self.__bound_model)

        output = {}

        use_records = False

        if 'count' in expand or context.returning == 'count':
            expand.pop('count', None)
            use_records = True
            output['count'] = self.count()

        if 'ids' in expand or context.returning == 'ids':
            expand.pop('ids', None)
            use_records = True
            output['ids'] = self.ids()

        if 'first' in expand or context.returning == 'first':
            expand.pop('first', None)
            use_records = True
            record = self.first()
            output['first'] = record.__json__() if record else None

        if 'last' in expand or context.returning == 'last':
            expand.pop('last', None)
            use_records = True
            record = self.last()
            output['last'] = record.__json__() if record else None

        if not output or (expand and context.returning not in ('count', 'ids', 'first', 'last')):
            records = [r.__json__() if hasattr(r, '__json__') else r for r in self]
            if not use_records:
                return records
            else:
                output['records'] = records

        return output

    def __len__(self):
        """
        Returns the length of this collection.  See also the `count` method.

        :return: <int>
        """
        return self.count()

    def __iter__(self):
        """
        Implements the iteration method to allow iteration over the
        records within this collection.

        :return: <generator>
        """
        if self.is_null():
            return

        context = self.context()

        # first, look to see if we have already cached the results
        # of this collection's query -- if we have, we can just iterate over
        # that object
        try:
            with ReadLocker(self.__lock):
                cached_records = self.__cache['records'][context]

        except KeyError:
            # otherwise, we need to get the raw values for this collection
            raw_records = self._fetch_data('records', context,
                                           lambda conn: conn.select(self.__bound_model, context))

            # loop through each raw value, process it
            # according to the context and yield it, collecting
            # the record cache.  we only want to store the cache
            # if ALL the records have finished processing.  if
            # the loop is exited prematurely, the cache should not be stored
            processed_records = []
            for raw_record in raw_records:
                processed_record = self._process_record(raw_record, context)
                yield processed_record
                processed_records.append(processed_record)

            with WriteLocker(self.__lock):
                self.__cache['records'][context] = processed_records

        # if we have the cached values for this context,
        # then simply loop through and yield the new values
        else:
            for cached_record in cached_records:
                yield cached_record

    def __getitem__(self, index):
        """
        Implements the Python __getitem__ built-in to support accessing records
        by index and slicing.  When slicing a collection, you will be returned
        a sub-collection instance with the start and limit values set.  When
        accessing an individual index, it is the same as calling the `at` method,
        only it will raise an IndexError if the record is not found vs. returning
        None.

        :return: <variant>
        """
        if isinstance(index, slice):
            start = index.start or 0
            stop = index.stop or 0

            # calculate the limit based on the stop index
            if index.stop <= 0:
                limit = (self.count() + stop) - start
            else:
                limit = index.stop - start

            return self.copy(start=start, limit=limit)
        else:
            records = self.records()
            return records[index]

    def __nonzero__(self):
        return not self.is_empty()

    def _cache_record(self, record, context):
        """
        Caches the given record within a context for this collection.

        :param record: <variant>
        :param context: <orb.Context>
        """
        try:
            with ReadLocker(self.__lock):
                records = self.__cache['records'][context]
        except KeyError:
            # ensure that the data we are storing matches up
            if isinstance(record, orb.Model):
                if self.__bound_model is None:
                    self.__bound_model = type(record)
                elif type(record) != self.__bound_model:
                    raise NotImplementedError('Cannot store multiple kinds of models in one collection')

            records = []
            with WriteLocker(self.__lock):
                self.__cache['records'][context] = records

        # adds the record to the cache
        records.append(record)

    def _fetch_data(self, key, context, func):
        """
        Returns the raw backend values for the given method, key and
        context combination.  This method will look through the preloaded
        backend values first, and query the backend second.

        :param key: <str>
        :param context: <orb.Context>
        :param func: <callable>

        :return: <variant>
        """
        try:
            with ReadLocker(self.__lock):
                return self.__cache['preload_{0}'.format(key)][context]
        except KeyError:
            conn = context.db.connection()
            return func(conn)

    def _create_records(self, records, context):
        """
        Generates a response of ids and records based on the given input.

        :param records: <list> or <orb.Collection>
        :param context: <orb.Context>

        :return: <generator> <variant> id, <orb.Model> record
        """
        # generate records off the list of models
        if isinstance(records, (list, set, tuple)):
            if self.__bound_model:
                id_col = self.__bound_model.schema().id_column()
            else:
                id_col = None

            for record in records:
                # given a dictionary object, determine if we need
                # to create a new record
                if isinstance(record, dict):
                    if not self.__bound_model:
                        raise orb.errors.ModelNotFound('None')

                    record_attributes = record
                    record_id = record_attributes.pop(id_col.name(), None)

                    # if no record id was defined in the data,
                    # then we need to create a new record
                    if not record_id:
                        if self.__bound_collector:
                            record = self.__bound_collector.create_record(self.__bound_source_record,
                                                                          record_attributes,
                                                                          context=context)
                        else:
                            record = self.__bound_model.create(record_attributes, context=context)

                    # otherwise, we will lookup the existing record based on the
                    # data provided
                    else:
                        record = self.__bound_model(record_id, context=context)
                        if record_attributes:
                            record.update(record_attributes)
                            record.save()

                yield record.id(), record

        # generate records off a collection
        elif isinstance(records, orb.Collection):
            for record in records:
                yield record.id(), record

        # raise an error for an invalid input
        else:
            raise orb.errors.OrbError('Invalid input for record generation')

    def _process_record(self, raw_record, context):
        """
        Processes an individual raw record and returns the desired response
        based on the given context.  This method implements the same logic as
        `_process_records` however is optimized for an individual record vs. a list.

        :param raw_record: <dict>
        :param context: <orb.Context>

        :return: <variant>
        """
        # if the context is expecting inflated values (full model objects) and the
        # return type is not values (a list of values) or data (a dictionary object)
        # then inflate each record to the bound model for this collection
        if context.returning not in ('values', 'data'):
            return self.__bound_model.restore_record(raw_record, context=context)

        # otherwise, if we have limited the context to a particular set
        # of columns, then filter down the response for that
        elif context.columns:
            schema = self.__bound_model.schema()
            cols = [schema.column(col) for col in context.columns]

            # for value specific returns, we need to filter
            # down the data to only having the value - not the column key
            # requesting a value with a single column will return a list of
            # values, requesting a value with multiple columns will return
            # a list of value tuples
            if context.returning == 'values':
                if len(cols) == 1:
                    return raw_record.get(cols[0].field())

                else:
                    return tuple(raw_record.get(col.field()) for col in cols)

            # otherwise, if the request is asking for data vs. values
            # then we just need to filter down the response to the
            # key value pairing for just the requested columns
            else:
                return {col.field(): raw_record.get(col.field()) for col in cols}

        # it no columns are specified, then we can simply return the raw records
        # in the form we received them from the backend
        else:
            return raw_record

    def add(self, record, **context):
        """
        Adds a new record to the collection.  If this collection has bound collectors
        then it will dynamically generate relationships between the model and it's
        collector (see the `Collector` documentation for more information).

        If this collection is an in-memory only instance, then it will add the
        record to the stored cache.

        :param record: <orb.Model>
        :param context: <orb.Context>
        """
        orb_context = self.context(**context)

        # check to see if we have a bound collector, and if we do, then
        # run the add record logic for it
        if self.__bound_collector is not None:
            result = self.__bound_collector.add_record(self.__bound_source_record,
                                                       record,
                                                       context=orb_context)
            self.clear_cache()
            return result

        # otherwise we will add the record to our own cache
        else:
            self._cache_record(record, orb_context)
            return True

    def at(self, index, **context):
        """
        Returns the record at the given index.  The first time this method is called on an
        un-loaded collection will cause the collection to fetch all of the contents.  Afterwards,
        it will access the record at the given index location.  Multiple calls to this method
        will not incur any additional overhead, but may not be the most optimized way to
        access only a single record.  Consider using slicing if you are trying to access
        a single record in the middle of a collection that requires a backend lookup.

        :param index: <int>
        :param context: <orb.Context>
        """
        records = self.records(**context)
        try:
            return records[index]
        except IndexError:
            return None

    def bind_collector(self, collector):
        """
        Binds this collection as being generated from the given
        collector.  This method will return a pointer to itself
        to allow for easy chaining.

        :param collector: <orb.Collector>

        :return: <orb.Collection> (self)
        """
        self.__bound_collector = collector
        return self

    def bind_model(self, model):
        """
        Binds this collection to the given model subclass.  This
        will signify that the collection represents the given model
        type's records.  This method will return a pointer to itself
        to allow for easy chaining.

        :param model: subclass of <orb.Model>

        :return: <orb.Collection> (self)
        """
        self.__bound_model = model
        return self

    def bind_source_record(self, record):
        """
        Binds this collection to the given record instance.  This method
        will return a pointer to itself to allow for easy chaining.

        :param record: <orb.Model>

        :return: <orb.Collection> (self)

        :usage

            from orb import Collection

            user = User.select().first()
            groups = Collection().bind_source_record(user).bind_model(Group)
        """
        self.__bound_source_record = record
        return self

    def clear_cache(self):
        """
        Clears the cache for this collection.
        """
        with WriteLocker(self.__lock):
            self.__cache = defaultdict(dict)

    def create(self, values, **context):
        """
        Takes the given record values and creates a new record
        from the bound model or collector and returns it.

        :param values: <dict>
        :param context: <orb.Context> descriptor

        :return: <orb.Model>
        """
        # create a model through the bound collector of this collection
        if self.__bound_collector:
            record = self.__bound_collector.create_record(self.__bound_source_record,
                                                          values,
                                                          **context)

            # clear for bound connections, so that the
            # cache can be reloaded after the backend modification
            self.clear_cache()

        # otherwise, create a record from the bound model for this collection
        else:
            record = self.__bound_model.create(values, **context)

            # add to the cache for unbound connections
            self._cache_record(record, self.context(**context))

        return record

    def context(self, **context):
        """
        Creates a new or returns the existing context, based on if
        there is any modification information about the context.

        :param context: <orb.Context> descriptor

        :return: <orb.Context>
        """
        if context:
            new_context = self.__context.copy()
            new_context.update(context)
            return new_context
        else:
            return self.__context

    def copy(self, **context):
        """
        Creates a copy of this collection.  This will copy over
        the valid cache and preload information to the new instance.

        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        my_context = self.context()
        sub_context = self.context(**context)

        # determine which internal cached information can be re-used
        with ReadLocker(self.__lock):
            records = self.__cache['records'].get(sub_context)

        if not records:
            with ReadLocker(self.__lock):
                my_records = self.__cache['records'].get(my_context)

            # if the only difference between the two contexts is the slicing of this
            # collection, then we can re-use the cached values
            diff = my_context.difference(sub_context)
            if my_records and (diff == {'start', 'limit'} or
                               diff == {'page', 'page_size'} or
                               diff == {'page'}):
                start_index = sub_context.start
                end_index = start_index + sub_context.limit
                records = my_records[start_index:end_index]

        # generate the new collection
        other = orb.Collection(records=records, context=context)

        if self.__bound_collector:
            other.bind_collector(self.__bound_collector)
        if self.__bound_model:
            other.bind_model(self.__bound_model)
        if self.__bound_source_record:
            other.bind_source_record(self.__bound_source_record)

        # transfer over the preload cache for the copy
        for key, context_values in self.__cache.items():
            if key.startswith('preload_'):
                for context, value in context_values.items():
                    preload_key = key.replace('preload_', '')
                    other.preload_data({preload_key: value}, context=context)

        return other

    def count(self, **context):
        """
        Returns the size or count of records that this collection contains.

        :param context: <orb.Context> descriptor

        :return: <int>
        """
        if self.is_null():
            return 0

        orb_context = self.context(**context)

        # determine if we have any cached information
        # that we can use first to derive count
        try:
            with ReadLocker(self.__lock):
                return self.__cache['count'][orb_context]
        except KeyError:
            try:
                with ReadLocker(self.__lock):
                    return len(self.__cache['records'][orb_context])

            except KeyError:
                try:
                    with ReadLocker(self.__lock):
                        count = self.__cache['preload_count'][orb_context] or 0
                except KeyError:
                    try:
                        with ReadLocker(self.__lock):
                            raw = self.__cache['preload_records'][orb_context] or []
                            count = len(raw)

                    # if no existing cache can be used, then query
                    # the backend
                    except KeyError:
                        # create an optimized query lookup based
                        # off this context
                        optimized_context = orb_context.copy()
                        optimized_context.columns = [self.__bound_model.schema().id_column()]
                        optimized_context.expand = None
                        optimized_context.order = None

                        conn = optimized_context.db.connection()
                        count = conn.count(self.__bound_model, optimized_context)

                # cache the results for future lookup
                with WriteLocker(self.__lock):
                    self.__cache['count'][orb_context] = count
                return count

    def delete(self, **context):
        """
        Deletes this collection from the database.

        :param context: <orb.Context>
        """
        orb_context = self.context(**context)

        # delete records from a collector that defines it
        if self.__bound_collector:
            try:
                processed, count = self.__bound_collector.delete_records(self, context=orb_context)
            except NotImplementedError:
                processed, count = None, 0
        else:
            processed = None
            count = 0

        # if there is no bound collector, or the collector does
        # not process the deletion event, then delete directly
        if processed is None:
            records = self.records(context=orb_context)
            delete_records = list(orb.events.DeleteEvent.process(records))
            if delete_records:
                conn = orb_context.db.connection()
                count = conn.delete(delete_records, orb_context)[1]

        return count

    def distinct(self, *columns, **context):
        """
        Selects the distinct column values for the given columns for
        this method.

        :param columns: vargs list of string column names to query for
        :param context: <orb.Context> descriptor

        :return: [<variant>, ..]
        """
        context['distinct'] = columns
        return self.values(*columns, **context)

    def empty(self, **context):
        """
        Empties out this collection, deleting all the records from
        the backend that are associated with it.

        :return: <int>
        """
        if not self.is_null():
            count = self.update([], **context)
        else:
            count = 0

        return count

    def first(self, **context):
        """
        Returns the first record in the sequence for this collection.

        :param context: <orb.Context> descriptor

        :return: <variant>
        """
        if self.is_null():
            return None

        orb_context = self.context(**context)

        # first, return the cached content
        try:
            with ReadLocker(self.__lock):
                return self.__cache['first'][orb_context]

        except KeyError:
            # secondly, return from the cached record content the
            # first index, if possible
            try:
                with ReadLocker(self.__lock):
                    return self.__cache['records'][orb_context][0]

            # an index error implies that we had a records cache
            # but it was empty.  if we have a cache, we can assume
            # that we've already done the required loading
            except IndexError:
                return None

            # a key error implies that we don't have any cached
            # records yet, so we can load the first record directly
            except KeyError:
                try:
                    with ReadLocker(self.__lock):
                        raw = self.__cache['preload_first'][orb_context]
                except KeyError:
                    schema = self.__bound_model.schema()

                    # ensure we don't modify our context
                    if not context:
                        orb_context = orb_context.copy()

                    orb_context.limit = 1
                    if not orb_context.order:
                        orb_context.order = [(schema.id_column().name(), 'asc')]

                    records = self.records(context=orb_context)
                    record = records[0] if records else None
                else:
                    record = self._process_record(raw, orb_context)

                # cache the output response for future use
                with WriteLocker(self.__lock):
                    self.__cache['first'][orb_context] = record
                return record

    def grouped(self, *columns, **context):
        """
        Groups the records contained in this collection together
        as a nested set of dictionaries.  If the optional `preload`
        keyword is True, then all of the data will be loaded at once
        and then grouped together, otherwise the nested group will
        contain queries and sub-collections.

        :param columns: vargs list of columns to group by
        :param context: <orb.Context> descriptor

        :return: {<variant> column: { .. : <variant> value, .. }, ..}
        """
        preload = context.pop('preload', False)

        output = {}

        # preload all the records and then group them together
        if preload or self.is_loaded(**context):
            records = self.records(**context)

            for record in records:
                data = output
                for column in columns[:-1]:
                    key = record[column]
                    data.setdefault(key, {})
                    data = data[key]

                key = record[columns[-1]]
                data.setdefault(key, orb.Collection())
                data[key].add(record)

        # create sub-collections grouped by query
        else:
            values = self.distinct(*columns, **context)

            for value in values:
                data = output
                q = orb.Query()

                if len(columns) == 1:
                    key = value
                else:
                    for i, column in enumerate(columns[:-1]):
                        key = value[i]
                        data.setdefault(key, {})
                        data = data[key]

                        q &= orb.Query(column) == key

                    key = value[-1]

                q &= orb.Query(columns[-1]) == key

                group_context = context.copy()
                group_context['where'] = q & group_context.get('where')
                data.setdefault(key, self.refine(**group_context))

        return output

    def has(self, record, **context):
        """
        Returns whether or not this record has the given record or not.

        :param record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        orb_context = self.context(**context)

        # check the cache against the record
        try:
            with ReadLocker(self.__lock):
                records = self.__cache['records'][orb_context]

        # lookup from the backend to see if we have the record
        except KeyError:
            orb_context.where = orb.Query(self.__bound_model) == record
            return self.count(context=orb_context) > 0

        else:
            return record in records

    def ids(self, **context):
        """
        Returns a list of the ids for the records within this collection.

        :param context: <orb.Context> descriptor

        :return: [<variant> record_id, ..]
        """
        if self.is_null():
            return []

        orb_context = self.context(**context)

        # first try to access the cached content
        try:
            with ReadLocker(self.__lock):
                return self.__cache['ids'][orb_context]

        except KeyError:
            # otherwise, lookup the preloaded content
            try:
                with ReadLocker(self.__lock):
                    ids = self.__cache['preload_ids'][orb_context] or []

            # finally query the backend for the id content
            except KeyError:
                id_column = self.__bound_model.schema().id_column()
                ids = self.values(id_column, context=orb_context)

            # cache the results
            with WriteLocker(self.__lock):
                self.__cache['ids'][orb_context] = ids

            return ids

    def index(self, record, **context):
        """
        Returns the index location of the given record.  This method
        will lookup all records for this collection if not already
        loaded before calculating it's index.

        :param record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <int>
        """
        orb_context = self.context(**context)
        try:
            with ReadLocker(self.__lock):
                cached_records = self.__cache['records'][orb_context]
        except KeyError:
            ids = self.ids()
            return ids.index(record.id())
        else:
            return cached_records.index(record)

    def is_empty(self, **context):
        """
        Returns whether or not the collection is empty
        for the given context.  Empty means that there
        are no records found for the collection.

        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        return self.count(**context) == 0

    def is_loaded(self, **context):
        """
        Returns whether or not the context has already
        been loaded for this collection.  Loaded means
        that a cache for the records has already been created.

        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        orb_context = self.context(**context)
        with ReadLocker(self.__lock):
            return orb_context in self.__cache['records']

    def is_null(self, **context):
        """
        Returns whether or not the collection has no defining attributes.
        This means that no local cache has been created, and no bound
        model is defined for this model.

        :param context: <orb.Context> descriptor

        :return: <bool>
        """
        with ReadLocker(self.__lock):
            return len(self.__cache) == 0 and self.__bound_model is None

    def iter_batches(self, size=100):
        """
        Generates a batch iterator across the collection's data.

        :param size: <int>

        :return: <orb.BatchIterator>
        """
        return BatchIterator(self, size)

    def iter_records(self, **context):
        """
        Iterates over the records in this collection.

        :param context: <orb.Context> descriptor

        :return: <generator>
        """
        if self.is_null():
            return

        orb_context = self.context(**context)

        try:
            with ReadLocker(self.__lock):
                records = self.__cache['records'][orb_context] or []
        except KeyError:
            # look to see if we have preloaded records that can be used
            try:
                with ReadLocker(self.__lock):
                    raw_records = self.__cache['preload_records'][orb_context] or []

            # finally, select the results from the backend
            except KeyError:
                conn = orb_context.db.connection()
                raw_records = conn.select(self.__bound_model, orb_context) or []

            for raw_record in raw_records:
                yield self._process_record(raw_record, orb_context)
        else:
            for record in records:
                yield record

    def iter_values(self, *columns, **context):
        """
        Iterates over values from records in this collection.

        :param columns: [<str> or <orb.Column>, ..]
        :param context: <orb.Context> descriptor

        :return: <generator>
        """
        if self.is_null():
            return

        is_single = len(columns) == 1
        records_context = context.copy()
        records_context.pop('returning', None)
        for record in self.iter_records(**records_context):
            if is_single:
                yield record.get(columns[0], **context)
            else:
                yield tuple(record.get(c, **context) for c in columns)

    def last(self, **context):
        """
        Returns the last record in this collection.

        :param context: <orb.Context>  descriptor

        :return: <variant>
        """
        if self.is_null():
            return None

        orb_context = self.context(**context)

        # first lookup based on the previous cache
        try:
            with ReadLocker(self.__lock):
                return self.__cache['last'][orb_context]

        except KeyError:
            # second, look to see if we have already
            # cached a full record list and return the
            # last entry from it
            try:
                with ReadLocker(self.__lock):
                    return self.__cache['records'][orb_context][-1]
            except KeyError:

                # finally, process the raw record from a preloaded
                # cache or look it up from the backend
                try:
                    with ReadLocker(self.__lock):
                        raw_record = self.__cache['preload_last'][orb_context]
                except KeyError:
                    if not orb_context.order:
                        schema = self.__bound_model.schema()
                        order = [(schema.id_column().name(), 'desc')]
                        record = self.ordered(order).first(context=orb_context)
                    else:
                        record = self.reversed().first(context=orb_context)
                else:
                    record = self._process_record(raw_record, orb_context)

                # cache the result for future use
                with WriteLocker(self.__lock):
                    self.__cache['last'][orb_context] = record
                return record

    def model(self):
        """
        Returns the model that has been bound to this collection, if any.

        :return: subclass of <orb.Model> or None
        """
        return self.__bound_model

    def ordered(self, order):
        """
        Return a copy of this collection with the new order sequence.

        :param order:  <str> || (<str>, <str> 'ASC' || 'DESC')

        :return: <orb.Collection>
        """
        return self.copy(order=order)

    def page(self, number, **context):
        """
        Returns the records for the current page, or the specified page number.
        If a page size is not specified, then this record sets page size will
        be used.

        :param number: <int>

        :return     <orb.RecordSet>
        """
        size = max(0, self.context(**context).page_size)
        if not size:
            return self.copy()
        else:
            return self.copy(page=number, page_size=size)

    def page_count(self, **context):
        """
        Returns the number of pages within this collection.  This will
        use the `page_size` context attribute, either built-in to the
        context already defined or provided within the context options
        to the method.

        :param context: <orb.Context> descriptor

        :return: <int>
        """
        size = max(0, self.context(**context).page_size)

        if not size:
            return 1
        else:
            context['page'] = None
            context['page_size'] = None

            fraction = self.count(**context) / float(size)
            count = int(math.ceil(fraction))
            return max(1, count)

    def preload_data(self, cache, **context):
        """
        Adds preloaded data to this collection.  This will provide
        raw backend data to the collection for a particular context

        :param cache: {<str> key: <variant> value, ..}
        """
        orb_context = self.context(**context)
        with WriteLocker(self.__lock):
            for key, value in cache.items():
                self.__cache['preload_' + key][orb_context] = value

    def records(self, **context):
        """
        Looks up and generates a list of records for this collection.

        :param context: <orb.Context> descriptor

        :return: [<variant> record, ..]
        """
        if self.is_null():
            return []

        orb_context = self.context(**context)
        try:
            with ReadLocker(self.__lock):
                return self.__cache['records'][orb_context]
        except KeyError:
            records = list(self.iter_records(**context))
            with WriteLocker(self.__lock):
                self.__cache['records'][orb_context] = records
            return records

    def refine(self, create_new=True, **context):
        """
        Refines this collection based on the given context.  If the
        optional create_new keyword is provided and set to False, this
        will refine the context for this collection in-place and not
        generate a new collection object.  If not, the method
        is functionally the same as the `copy` method.

        :param create_new: <bool>
        :param context: <orb.Context>

        :return: <orb.Collection>
        """
        if not create_new:
            self.__context.update(context)
            return self
        else:
            return self.copy(**context)

    def remove(self, record, **context):
        """
        Removes the given record from the collection.  If the collection
        is bound to a collector, then it will be removed from the backend.

        :param record: <orb.Model>
        :param context: <orb.Context> descriptor
        """
        orb_context = self.context(**context).sub_context()

        # remove the record from the collector instance
        if self.__bound_collector:
            self.__bound_collector.remove_record(self.__bound_source_record,
                                                 record,
                                                 context=orb_context)
            self.clear_cache()

        # remove the record from the internal cache
        else:
            try:
                with ReadLocker(self.__lock):
                    records = self.__cache['records'][orb_context]
            except KeyError:
                raise ValueError
            else:
                records.remove(record)

    def reversed(self):
        """
        Reverses the order of this collection and returns the
        results.

        :return: <orb.Collection>
        """
        orb_context = self.context()
        if not self.is_loaded():
            return self.refine(context=orb_context.reversed())
        else:
            with ReadLocker(self.__lock):
                records = list(reversed(self.__cache['records'][orb_context]))
            return orb.Collection(records)

    def update(self, records, use_method=True, **context):
        """
        Updates this collection and assigns the records to the collection.

        :param records: [<orb.Model>, ..]
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: [<orb.Model>, ..]
        """
        orb_context = self.context(**context)
        create_context = orb_context.sub_context()

        try:
            setter = self.__bound_collector.settermethod() if use_method else None
        except AttributeError:
            setter = None

        # use the setter method to update this collection
        if setter:
            return setter(self.__bound_source_record, records, context=create_context)
        else:
            if isinstance(records, dict):
                if 'ids' in records:
                    record_info = records['ids']
                elif 'records' in records:
                    record_info = records['records']
                else:
                    raise orb.errors.OrbError('Invalid input for collection update')
            else:
                record_info = records

            if record_info:
                new_ids, new_records = zip(*list(self._create_records(record_info, create_context)))
            else:
                new_ids, new_records = ([], [])

            # update the root collector information
            if self.__bound_collector:
                self.__bound_collector.update_records(self.__bound_source_record,
                                                      self,
                                                      new_ids,
                                                      context=create_context)
                self.clear_cache()

            # update the record cache
            else:
                if new_records and isinstance(new_records[0], orb.Model):
                    self.__bound_model = type(new_records[0])

                with WriteLocker(self.__lock):
                    self.__cache['records'][orb_context] = new_records

            return new_records

    def save(self, **context):
        """
        Saves the records for this collection.  This method will iterate through
        each record and run the save event before saving the collection to the
        backend.

        :param context: <orb.Context>

        :return: <bool> saved
        """
        orb_context = self.context(**context)
        iter = self if not context else self.records(**context)

        # pre-process the save event
        create_records = []
        update_records = []
        for record in iter:
            event = orb.events.SaveEvent(
                context=orb_context,
                new_record=not record.is_record(),
            )
            record.on_pre_save(event)
            if not event.prevent_default:
                if event.new_record:
                    create_records.append(record)
                else:
                    update_records.append(record)

        if not (update_records or create_records):
            return False
        else:
            conn = orb_context.db.connection()

            # create new records into the backend
            if create_records:
                results, _ = conn.insert(create_records, orb_context)

                # store the newly generated ids
                for i, record in enumerate(create_records):
                    record.parse(results[i])

            # update existing records in the backend
            if update_records:
                conn.update(update_records, orb_context)

            # run the post-commit event for each record
            for record in create_records + update_records:
                event = orb.events.SaveEvent(
                    context=orb_context,
                    new_record=record in create_records
                )
                record.on_post_save(event)

            return True

    def search(self, terms, **context):
        """
        Performs additional search on this collection, joining
        any already existing search context with
        the search results from the model's search engine.

        :param terms: <str>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        model = self.model()
        engine = model.get_search_engine()
        orb_context = self.context(**context)
        return engine.search(model, terms, context=orb_context)

    def values(self, *columns, **context):
        """
        Returns a list of values for the given columns from this collection.
        If given a single column, the returned results will be a list of values.
        If given multiple columns, the returned results will be a list of tuples
        of values per record.

        :param columns: vargs column name or objects
        :param context: <orb.Context> descriptor

        :return: [<variant> value(s), ..]
        """
        if self.is_null():
            return []

        # create a copy of the context to cache
        cache_context_data = context.copy()
        cache_context_data['columns'] = columns
        cache_context = self.context(**cache_context_data)

        # first lookup the results from the local cache
        try:
            with ReadLocker(self.__lock):
                return self.__cache['values'][cache_context]
        except KeyError:
            values = list(self.iter_values(*columns, **context))
            with WriteLocker(self.__lock):
                self.__cache['values'][cache_context] = values
            return values

    # support deprecated calls (transitioning fully to PEP8)
    @deprecated
    def clear(self):
        """ Backward compatibility support, please use `clear_cache` now. """
        return self.clear_cache()

    @deprecated
    def isEmpty(self, **context):
        """ Backward compatibility support, please use `is_empty` now. """
        return self.is_empty(**context)

    @deprecated
    def isLoaded(self, **context):
        """ Backward compatibility support, please use `is_loaded` now. """
        return self.is_loaded(**context)

    @deprecated
    def isNull(self, **context):
        """ Backward compatibility support, please use `is_null` now. """
        return self.is_null(**context)

    @deprecated
    def pageCount(self, **context):
        """ Backward compatibility support, please use `page_support` now. """
        return self.page_count(**context)
