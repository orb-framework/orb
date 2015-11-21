from projex.lazymodule import lazy_import
from projex.locks import ReadWriteLock, ReadLocker, WriteLocker

orb = lazy_import('orb')


class CollectionIterator(object):
    def __init__(self, collection, batch=1):
        self.__collection = collection
        self.__page = 1
        self.__index = -1
        self.__pageSize = batch
        self.__records = []

    def __iter__(self):
        return self

    def next(self):
        self.__index += 1

        # get the next batch of records
        if len(self.__records) <= self.__index:
            self.__records = list(self.__collection.page(self.__page, pageSize=self.__pageSize))
            self.__page += 1
            self.__index = 0

        # stop the iteration when complete
        if not self.__records:
            raise StopIteration()
        else:
            return self.__records[self.__index]


class Collection(object):
    def __json__(self):
        records = self.records()
        return [record.__json__() for record in self.records()]

    def __init__(self, records=None, model=None, source='', record=None, pipe=None, **context):
        self.__cacheLock = ReadWriteLock()
        self.__cache = {
            'first': {},
            'last': {},
            'records': {},
            'count': {}
        }
        self.__context = orb.Context(**context)
        self.__model = model
        self.__source = source
        self.__record = record
        self.__pipe = pipe

        if records is not None:
            self.__cache['records'][self.__context] = records

    def __len__(self):
        return self.count()

    def __iter__(self):
        for record in self.records():
            yield record

    def __getitem__(self, index):
        record = self.at(index)
        if not record:
            raise IndexError(index)
        else:
            return record

    def add(self, record):
        if self.pipe():
            cls = self.pipe().throughModel()
            data = {}
            data[self.pipe().from_()] = self.__record
            data[self.pipe().to()] = record
            new_record = cls(data)
            new_record.save()
            return new_record
        else:
            raise NotImplementedError

    def at(self, index, **context):
        records = self.records(**context)
        try:
            return records[index]
        except IndexError:
            return None

    def clear(self):
        with WriteLocker(self.__cacheLock):
            self.__cache = {
                'first': {},
                'last': {},
                'records': {},
                'count': {}
            }

    def create(self, values, **context):
        # create a new pipe object
        if self.pipe():
            target_model = self.pipe().targetModel()
            target_col = self.pipe().targetColumn()

            # add the target based on the name or field
            if target_col.name() in values or target_col.field() in values:
                record = values.get(target_col.name(), values.get(target_col.field()))
                if not isinstance(record, orb.Model):
                    record = target_model(record)
                self.add(record)
            else:
                record = target_model(values)
                record.save()
                self.add(record)

        # create a new record for this collection
        else:
            if self.__source:
                values.setdefault(self.__source, self.__record)
            return self.__model.create(values, **context)

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
            model=self.__model,
            record=self.__record,
            source=self.__source,
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
            context.columns = [self.__model.schema().idColumn()]
            context.expand = None
            context.order = None

            conn = context.db.connection()
            count = conn.count(self.__model, context)

            with WriteLocker(self.__cacheLock):
                self.__cache['count'][context] = count
            return count

    def delete(self, **context):
        context = orb.Context(**context)
        pipe = self.pipe()

        # delete piped records
        if pipe:
            through = pipe.throughModel()

            # collect the ids that are within this pipe
            base_context = self.context()
            ids = self.ids()

            # remove them from the system
            q = orb.Query(pipe.target()).in_(ids)
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

    def first(self, **context):
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
                context.limit = 1
                context.order = '-id'
                records = self.records(context=context)
                record = records[0] if records else None
                with WriteLocker(self.__cacheLock):
                    self.__cache['first'][context] = record
                return record

    def hasRecord(self, record, **context):
        context = self.context(**context)
        context.returning = 'values'
        context.columns = ['id']
        return self.first(context=context) is not None

    def ids(self, **context):
        return self.records(columns=['id'], returning='values', **context)

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

    def iterate(self, batch=1):
        return CollectionIterator(self, batch)

    def last(self, **context):
        context = self.context(context)
        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['last'][context]
        except KeyError:
            record = self.reversed().first(**context)
            with WriteLocker(self.__cacheLock):
                self.__cache['last'][context] = record
            return record

    def model(self):
        return self.__model

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
            fraction = self.totalCount(**context) / float(size)
            count = int(fraction)
            if count % 1:
                count += 1
            return count

    def pipe(self):
        return self.__pipe

    def records(self, **context):
        context = self.context(**context)

        try:
            with ReadLocker(self.__cacheLock):
                return self.__cache['records'][context]
        except KeyError:
            conn = context.db.connection()
            raw = conn.select(self.__model, context)

            if context.inflated and context.returning != 'values':
                records = [self.__model.inflate(x, context=context) for x in raw]
            elif context.columns:
                schema = self.__model.schema()
                if context.returning == 'values':
                    if len(context.columns) == 1:
                        col = schema.column(context.columns[0])
                        records = [x[col.field()] for x in raw]
                    else:
                        cols = [schema.column(col) for col in context.columns]
                        records = [[r.get(col.field()) for col in cols] for r in raw]
                else:
                    cols = [schema.column(col) for col in context.columns]
                    records = [{col.field(): r.get(col.field()) for col in cols} for r in raw]
            else:
                records = raw

            with WriteLocker(self.__cacheLock):
                self.__cache['records'][context] = records
            return records

    def refine(self, **context):
        context = self.context(**context)
        with ReadLocker(self.__cacheLock):
            records = self.__cache['records'].get(context)

        other = orb.Collection(
            records=records,
            model=self.__model,
            record=self.__record,
            source=self.__source,
            context=context
        )
        return other

    def remove(self, record, **context):
        pipe = self.pipe()
        if pipe:
            through = pipe.throughModel()
            q  = orb.Query(pipe.from_()) == self.__record
            q &= orb.Query(pipe.to()) == record

            context['where'] = q & context.get('where')
            context = self.context(**context)

            records = through.select(context=context)
            delete = []
            for record in records:
                event = orb.events.DeleteEvent()
                record.onDelete(event)
                if not event.preventDefault:
                    delete.append(record)

            conn = context.db.connection()
            conn.delete(records, context)

            return len(delete)
        else:
            raise NotImplementedError

    def search(self, terms, **context):
        """
        Searches for records within this collection based on the given terms.

        :param terms: <str>
        :param context: <orb.Context>

        :return: <orb.SearchResultCollection>
        """
        return self.model().searchEngine().search(terms, self.context(**context))

    def update(self, records, **context):
        if self.pipe():
            if 'ids' in records:
                ids = records.get('ids')
            elif 'records' in records:
                ids = [r.get('id') for r in records['records'] if x.get('id')]
            else:
                ids = records

            through = self.pipe().throughModel()
            pipe = self.pipe()
            curr_ids = self.ids()
            remove_ids = set(curr_ids) - set(ids)
            add_ids = set(ids) - set(curr_ids)

            # remove old records
            if remove_ids:
                q  = orb.Query(through, pipe.from_()) == self.__record
                q &= orb.Query(through, pipe.to()).in_(remove_ids)
                through.select(where=q).delete()

            # create new records
            if add_ids:
                collection = orb.Collection([through({pipe.from_(): self.__record, pipe.to(): id})
                                             for id in add_ids])
                collection.save()

            return len(add_ids), len(remove_ids)

        else:
            raise NotImplementedError

    def save(self, **context):
        records = self.records(**context)
        context = self.context(**context)
        conn = context.db.connection()

        create_records = []
        update_records = []

        # run the pre-commit event for each record
        for record in records:
            event = orb.events.SaveEvent()
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
                event = orb.events.LoadEvent(data=results[i])
                record.onLoad(event)

        if update_records:
            conn.update(update_records, context)

        # run the post-commit event for each record
        for record in records:
            event = orb.events.SaveEvent(result=True)
            record.onPostSave(event)

        return True

    def setModel(self, model):
        self.__model = model

    def setPipe(self, pipe):
        self.__pipe = pipe