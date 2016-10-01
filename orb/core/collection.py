import math

from collections import defaultdict
from projex.lazymodule import lazy_import
from projex.locks import ReadWriteLock, ReadLocker, WriteLocker


orb = lazy_import('orb')


class CollectionIterator(object):
    def __init__(self, collection, batch=1):
        self.__collection = collection
        self.__model = collection.model()
        self.__page = 1
        self.__index = -1
        self.__pageSize = batch
        self.__records = []

    def __iter__(self):
        return self

    def next(self):
        self.__index += 1

        # get the next batch of records
        if len(self.__records) in (0, self.__pageSize) and self.__index == len(self.__records):
            sub_collection = self.__collection.page(self.__page, pageSize=self.__pageSize, returning='values')
            self.__records = sub_collection.records()
            self.__page += 1
            self.__index = 0

        # stop the iteration when complete
        if not self.__records or self.__index == len(self.__records):
            raise StopIteration()
        else:
            if self.__collection.context().inflated in (True, None):
                return self.__model.inflate(self.__records[self.__index], context=self.__collection.context())
            else:
                return self.__records[self.__index]


class Collection(object):
    def __json__(self):
        context = self.context()
        expand = context.expandtree(self.__model)

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

    def __init__(self,
                 records=None,
                 model=None,
                 record=None,
                 collector=None,
                 preload=None,
                 **context):
        self.__cacheLock = ReadWriteLock()
        self.__cache = defaultdict(dict)
        self.__preload = preload or {}
        self.__context = orb.Context(**context)
        self.__model = model
        self.__record = record
        self.__collector = collector

        if records is not None and len(records) > 0:
            if self.__model is None:
                self.__model = type(records[0])

            self.__cache['records'][self.__context] = records

    def __len__(self):
        return self.count()

    def __iter__(self):
        if self.isNull():
            return

        context = self.context()

        try:
            with ReadLocker(self.__cacheLock):
                records = self.__cache['records'][context]

        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    raw = self.__preload['records'][context]

            except KeyError:
                conn = context.db.connection()
                raw = conn.select(self.__model, context)

            cache = []
            for x in self._process(raw, context):
                yield x
                cache.append(x)

            with WriteLocker(self.__cacheLock):
                self.__cache['records'][context] = cache

        else:
            for r in records:
                yield r

    def __getitem__(self, index):
        if isinstance(index, slice):
            return self.copy(start=index.start, limit=(index.stop - index.start))
        else:
            record = self.at(index)
            if not record:
                raise IndexError(index)
            else:
                return record

    def _process(self, raw, context):
        if context.inflated in (True, None) and context.returning not in ('values', 'data'):
            for x in raw or []:
                yield self.__model.inflate(x, context=context)

        elif context.columns:
            schema = self.__model.schema()
            if context.returning == 'values':
                if len(context.columns) == 1:
                    col = schema.column(context.columns[0])
                    for x in raw or []:
                        yield x[col.field()]

                else:
                    cols = [schema.column(col) for col in context.columns]
                    for x in raw or []:
                        yield tuple(x.get(col.field()) for col in cols)
            else:
                cols = [schema.column(col) for col in context.columns]
                for x in raw or []:
                    yield {col.field(): x.get(col.field()) for col in cols}
        else:
            for x in raw:
                yield x

    def add(self, record):
        if isinstance(self.__collector, orb.Pipe):
            cls = self.__collector.throughModel()
            data = {
                self.__collector.from_(): self.__record,
                self.__collector.to(): record
            }

            with WriteLocker(self.__cacheLock):
                self.__cache = defaultdict(dict)

            return cls.ensureExists(data, context=self.context())

        elif isinstance(self.__collector, orb.ReverseLookup):
            record.set(self.__collector.targetColumn(), self.__record)
            record.save()

            with WriteLocker(self.__cacheLock):
                self.__cache = defaultdict(dict)

            return True

        else:
            try:
                records = self.__cache['records'][self.__context]
            except KeyError:
                if self.__model is None or type(record) == self.__model:
                    self.__model = type(record)
                    self.__cache['records'][self.__context] = []
                else:
                    raise NotImplementedError

            records.append(record)
            return True

    def at(self, index, **context):
        records = self.records(**context)
        try:
            return records[index]
        except IndexError:
            return None

    def clear(self):
        with WriteLocker(self.__cacheLock):
            self.__cache = defaultdict(dict)

    def create(self, values, **context):
        # create a new pipe object
        if isinstance(self.__collector, orb.Pipe):
            target_model = self.__collector.toModel()
            target_col = self.__collector.toColumn()

            # add the target based on the name or field
            if target_col.name() in values or target_col.field() in values:
                record = values.get(target_col.name(), values.get(target_col.field()))
                if not isinstance(record, orb.Model):
                    record = target_model(record, context=orb.Context(**context))
                self.add(record)
            else:
                record = target_model(values, context=orb.Context(**context))
                record.save()
                self.add(record)

        # create a new record for this collection
        else:
            if isinstance(self.__collector, orb.ReverseLookup):
                values.setdefault(self.__collector.targetColumn().name(), self.__record)
            record = self.__model.create(values, **context)
            self.add(record)

        return record

    def collector(self):
        """
        Returns the collector that generated this collection, if any.

        :return:  <orb.Collector> || None
        """
        return self.__collector

    def context(self, **context):
        new_context = self.__context.copy()
        new_context.update(context)
        return new_context

    def copy(self, **context):
        context = self.context(**context)

        with ReadLocker(self.__cacheLock):
            records = self.__cache['records'].get(context)

        other = orb.Collection(
            records=records,
            preload=self.__preload,
            model=self.__model,
            record=self.__record,
            collector=self.__collector,
            context=context
        )
        return other

    def count(self, **context):
        if self.isNull():
            return 0

        context = self.context(**context)
        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['count'][context]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    return len(self.__cache['records'][context])
            except KeyError:
                optimized_context = context.copy()
                optimized_context.columns = [self.__model.schema().idColumn()]
                optimized_context.expand = None
                optimized_context.order = None

                try:
                    with ReadLocker(self.__cacheLock):
                        count = self.__preload['count'][context] or 0
                except KeyError:
                    try:
                        with ReadLocker(self.__cacheLock):
                            raw = self.__preload['records'][context] or []
                            count = len(raw)
                    except KeyError:
                        conn = optimized_context.db.connection()
                        count = conn.count(self.__model, optimized_context)

                with WriteLocker(self.__cacheLock):
                    self.__cache['count'][context] = count
                return count

    def delete(self, **context):
        context = orb.Context(**context)

        # delete piped records
        if isinstance(self.__collector, orb.Pipe):
            pipe = self.__collector
            through = pipe.throughModel()

            # collect the ids that are within this pipe
            base_context = self.context()
            ids = self.ids()

            # remove them from the system
            q = orb.Query(pipe.targetColumn()).in_(ids)
            base_context.where = q

            records = through.select(where=q, context=base_context)
            delete = []
            for record in records:
                event = orb.events.DeleteEvent()
                record.onDelete(event)
                if not event.preventDefault:
                    delete.append(record)

            conn = base_context.db.connection()
            return conn.delete(delete, base_context)[1]

        # delete normal records
        else:
            records = self.records(context=context)
            if not records:
                return 0

            # process the deletion event
            remove = []
            for record in records:
                event = orb.events.DeleteEvent()
                record.onDelete(event)
                if not event.preventDefault:
                    remove.append(record)

            # remove the records
            conn = context.db.connection()
            return conn.delete(remove, context)[1]

    def distinct(self, *columns, **context):
        context['distinct'] = columns
        return self.values(*columns, **context)

    def empty(self):
        if self.isNull():
            return 0
        else:
            return self.update([])

    def first(self, **context):
        if self.isNull():
            return None

        context = self.context(**context)

        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['first'][context]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    return self.__cache['first'][context]
            except IndexError:
                return None
            except KeyError:
                try:
                    with ReadLocker(self.__cacheLock):
                        raw = self.__preload['first'][context]
                except KeyError:
                    context.limit = 1
                    context.order = [(self.__model.schema().idColumn().name(), 'desc')]
                    records = self.records(context=context)
                    record = records[0] if records else None
                else:
                    record = self._process([raw], context).next()

                with WriteLocker(self.__cacheLock):
                    self.__cache['first'][context] = record
                return record

    def grouped(self, *columns, **context):
        preload = context.pop('preload', False)

        output = {}

        if preload:
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
        else:
            values = self.values(*columns, **context)

            for value in values:
                data = output
                q = orb.Query()
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
        context = self.context(**context)
        context.returning = 'values'
        context.columns = [self.__model.schema().idColumn().name()]
        return self.first(context=context) is not None

    def ids(self, **context):
        if self.isNull():
            return []

        context = self.context(**context)
        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['ids'][context]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    ids = self.__preload['ids'][context] or []
            except KeyError:
                ids = self.records(columns=[self.__model.schema().idColumn()],
                                   returning='values',
                                   context=context)

            with WriteLocker(self.__cacheLock):
                self.__cache['ids'][context] = ids

            return ids

    def index(self, record, **context):
        context = self.context(**context)
        if not record:
            return -1
        else:
            try:
                with ReadLocker(self.__cacheLock):
                    return self.__cache['records'][context].index(record)
            except KeyError:
                return self.ids().index(record.id())

    def isLoaded(self, **context):
        context = self.context(**context)
        with ReadLocker(self.__cacheLock):
            return context in self.__cache['records']

    def isEmpty(self, **context):
        return self.count(**context) == 0

    def isNull(self):
        with ReadLocker(self.__cacheLock):
            return self.__cache['records'].get(self.__context) is None and self.__model is None

    def iterate(self, batch=100):
        return CollectionIterator(self, batch)

    def last(self, **context):
        if self.isNull():
            return None

        context = self.context(**context)
        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['last'][context]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    raw = self.__preload['last'][context]
            except KeyError:
                record = self.reversed().first(context=context)
            else:
                record = self._process([raw], context).next()

            with WriteLocker(self.__cacheLock):
                self.__cache['last'][context] = record
            return record

    def model(self):
        return self.__model

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

        :param      pageno   | <int>
                    pageSize | <int>

        :return     <orb.RecordSet>
        """
        size = max(0, self.context(**context).pageSize)
        if not size:
            return self.copy()
        else:
            return self.copy(page=number, pageSize=size)

    def pageCount(self, **context):
        size = max(0, self.context(**context).pageSize)

        if not size:
            return 1
        else:
            context['page'] = None
            context['pageSize'] = None

            fraction = self.count(**context) / float(size)
            count = int(math.ceil(fraction))
            return max(1, count)

    def preload(self, cache, **context):
        context = self.context(**context)
        with WriteLocker(self.__cacheLock):
            for key, value in cache.items():
                self.__preload.setdefault(key, {})
                self.__preload[key][context] = value

    def records(self, **context):
        if self.isNull():
            return []

        context = self.context(**context)

        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['records'][context]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    raw = self.__preload['records'][context] or []
            except KeyError:
                conn = context.db.connection()
                raw = conn.select(self.__model, context)

            records = list(self._process(raw, context))

            with WriteLocker(self.__cacheLock):
                self.__cache['records'][context] = records
            return records

    def refine(self, createNew=True, **context):
        if not createNew:
            self.__context.update(context)
            return self
        else:
            context = self.context(**context)
            with ReadLocker(self.__cacheLock):
                records = self.__cache['records'].get(context)

            other = orb.Collection(
                records=records,
                model=self.__model,
                record=self.__record,
                collector=self.__collector,
                context=context
            )
            return other

    def remove(self, record, **context):
        if isinstance(self.__collector, orb.Pipe):
            pipe = self.__collector
            through = pipe.throughModel()
            q  = orb.Query(pipe.from_()) == self.__record
            q &= orb.Query(pipe.to()) == record

            my_context = self.context(**context)
            records = through.select(where=q, scope=my_context.scope)

            delete = []
            for r in records:
                event = orb.events.DeleteEvent()
                r.onDelete(event)
                if not event.preventDefault:
                    delete.append(r)

            if delete:
                conn = my_context.db.connection()
                conn.delete(delete, my_context)

            return len(delete)
        elif self.__collector:
            record.set(self.__collector.targetColumn(), None)
            record.save()
            return 1
        else:
            raise NotImplementedError

    def reversed(self):
        collection = self.copy()
        context = collection.context()
        order = [(col, 'asc' if dir == 'desc' else 'desc') for col, dir in context.order or []] or None
        collection.refine(order=order)
        return collection

    def update(self, records, useMethod=True, **context):
        if useMethod and self.__collector is not None and self.__collector.settermethod() is not None:
            return self.__collector.settermethod()(self.__record, records, **context)

        # clean up the records for removal
        if isinstance(records, dict):
            if 'ids' in records:
                return self.update(records['ids'])
            elif 'records' in records:
                return self.update(records['records'])
            else:
                raise orb.errors.OrbError('Invalid input for collection update: {0}'.format(records))
        else:
            output_records = []

            if isinstance(records, (list, set, tuple)):
                ids = []
                id_col = self.__model.schema().idColumn()

                for record in records:
                    if isinstance(record, dict):
                        record_attributes = record
                        record_id = record_attributes.pop(id_col.name(), None)
                        if not record_id:
                            if isinstance(self.__collector, orb.ReverseLookup):
                                reference_col = self.__collector.targetColumn()
                                reference_id = self.__record.id()
                                record_attributes.pop(reference_col.name(), None)
                                record_attributes[reference_col.field()] = reference_id

                            record = self.__model.create(record_attributes, **context).id()
                        else:
                            record = self.__model(record_id, **context)
                            if record_attributes:
                                record.update(record_attributes)
                                record.save()

                    output_records.append(record)

                    if isinstance(record, orb.Model):
                        ids.append(record.id())
                    else:
                        ids.append(record)

            elif isinstance(records, orb.Collection):
                ids = records.ids()
                output_records = records.records()

            else:
                raise orb.errors.OrbError('Invalid input for collection update: {0}'.format(records))

            # update a pipe
            if isinstance(self.__collector, orb.Pipe):
                pipe = self.__collector

                orb_context = self.context(**context)
                through = pipe.throughModel()
                curr_ids = self.ids()

                remove_ids = set(curr_ids) - set(ids)
                add_ids = set(ids) - set(curr_ids)

                # remove old records
                if remove_ids:
                    q  = orb.Query(through, pipe.from_()) == self.__record
                    q &= orb.Query(through, pipe.to()).in_(remove_ids)
                    orb_context.where = q
                    through.select(context=orb_context).delete()

                # create new records
                if add_ids:
                    collection = orb.Collection([
                        through({
                            pipe.from_(): self.__record,
                            pipe.to(): id
                        }, context=orb_context)
                        for id in add_ids
                    ])
                    collection.save()

            # udpate a reverse lookup
            elif isinstance(self.__collector, orb.ReverseLookup):
                orb_context = self.context(**context)
                source = self.__collector.targetColumn()
                model = source.schema().model()

                q = orb.Query(source) == self.__record
                if ids:
                    q &= orb.Query(model).notIn(ids)

                # determine the reverse lookups to remove from this collection
                remove = model.select(where=q, context=orb_context)

                # check the remove action to determine how to handle this situation
                if self.__collector.removeAction() == 'delete':
                    remove.delete()
                else:
                    for record in remove:
                        record.set(source, None)
                        record.save()

                # determine the new records to add to this collection
                if ids:
                    q  = orb.Query(model).in_(ids)
                    q &= (orb.Query(source) != self.__record) | (orb.Query(source) == None)

                    add = model.select(where=q, context=orb_context)
                    for record in add:
                        record.set(source, self.__record)
                        record.save()

            else:
                raise NotImplementedError

            # cache the output records
            with WriteLocker(self.__cacheLock):
                self.__preload.clear()
                self.__cache = defaultdict(dict)

            return output_records

    def save(self, **context):
        records = self.records(**context)
        context = self.context(**context)
        conn = context.db.connection()

        create_records = []
        update_records = []

        # run the pre-commit event for each record
        for record in records:
            event = orb.events.SaveEvent(context=context, newRecord=not record.isRecord())
            record.onPreSave(event)
            if not event.preventDefault:
                if record.isRecord():
                    update_records.append(record)
                else:
                    create_records.append(record)

        # save and update the records
        if create_records:
            results, _ = conn.insert(create_records, context)

            # store the newly generated ids
            for i, record in enumerate(create_records):
                record.update(results[i])

        if update_records:
            conn.update(update_records, context)

        # run the post-commit event for each record
        for record in create_records + update_records:
            event = orb.events.SaveEvent(context=context, newRecord=record in create_records)
            record.onPostSave(event)

        return True

    def setModel(self, model):
        self.__model = model

    def values(self, *columns, **context):
        if self.isNull():
            return []

        orig_context = context
        context = self.context(**orig_context)

        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['values'][(context, columns)]
        except KeyError:
            try:
                with ReadLocker(self.__cacheLock):
                    records = self.__cache['records'][context]
            except KeyError:
                try:
                    with ReadLocker(self.__cacheLock):
                        raw = self.__preload['records'][context] or []
                except KeyError:
                    context.columns = columns
                    conn = context.db.connection()
                    raw = conn.select(self.__model, context)

                schema = self.__model.schema()
                values = []
                fields = [schema.column(col) for col in columns]
                for record in raw:
                    if context.inflated is False:
                        record_values = [record[field.field()] for field in fields]
                    else:
                        record_values = []
                        for i, field in enumerate(fields):
                            col = columns[i]
                            raw_values = orig_context.copy()
                            raw_values['distinct'] = None

                            if isinstance(field, orb.ReferenceColumn) and raw_values.get('inflated') is None:
                                raw_values['inflated'] = col != field.field()

                            val = field.restore(record[field.field()], context=orb.Context(**raw_values))
                            record_values.append(val)

                    if len(fields) == 1:
                        values.append(record_values[0])
                    else:
                        values.append(record_values)

                with WriteLocker(self.__cacheLock):
                    self.__cache['values'][(context, columns)] = values
                return values

            # use preloaded cache for values when possible
            else:
                if len(columns) == 1:
                    return [record.get(columns[0]) if record else None for record in records]
                else:
                    return [(record.get(c) for c in columns) for record in records]
