from projex.lazymodule import lazy_import

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
            self.__records = list(self.__collection.page(self.__page, self.__pageSize))
            self.__page += 1
            self.__index = 0

        # stop the iteration when complete
        if not self.__records:
            raise StopIteration()
        else:
            return self.__records[self.__index]


class Collection(object):
    def __init__(self, records=None, model=None, source='', owner=None, **context):
        self.__cache = {
            'records': {},
            'count': {}
        }
        self.__context = orb.ContextOptions(**context)
        self.__pipe = None
        self.__model = None
        self.__source = ''
        self.__owner = None

        if records is not None:
            self.__cache['records'][self.__context] = records

    def __len__(self):
        return self.count()

    def __iter__(self):
        for record in self.records():
            yield record

    def add(self, record):
        if self.pipe():
            cls = self.pipe().throughModel()
            data = {}
            data[self.pipe().source()] = self.__owner
            data[self.pipe().target()] = record
            new_record = cls(data)
            new_record.commit()
            return new_record
        else:
            raise NotImplementedError

    def at(self, index, **context):
        records = self.records(**context)
        return records[index]

    def clear(self):
        self.__cache = {
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
                record.commit()
                self.add(record)

        # create a new record for this collection
        else:
            if self.__source:
                values.setdefault(self.__source, self.__owner)
            return self.__model.create(values, **context)

    def commit(self, **context):
        context = self.contextOptions(**context)
        conn = context.database.connection()

        create_records = []
        update_records = []

        # run the pre-commit event for each record
        records = self.records(**context)
        for record in records:
            event = orb.events.CommitEvent()
            record.onPreCommit(event)
            if not event.preventDefault:
                if record.isRecord():
                    update_records.append(record)
                else:
                    create_records.append(record)

        # save and update the records
        if create_records:
            conn.insert(create_records, context)
        if update_records:
            conn.update(update_records, context)

        # run the post-commit event for each record
        for record in records:
            event = orb.events.CommitEvent(result=True)
            record.onPostEvent(event)

        return True

    def contextOptions(self, **context):
        context = self.__context.copy()
        context.update(context)
        return context

    def copy(self, **context):
        context = self.contextOptions(**context)
        other = orb.Collection(
            records=self.__cache['records'].get(context),
            model=self.__model,
            owner=self.__owner,
            source=self.__source,
            context=context
        )
        return other

    def count(self, **context):
        context = self.contextOptions(**context)
        try:
            return self.__cache['count'][context]
        except KeyError:
            context.columns = [self.__model.schema().idField()]
            context.expand = None
            context.order = None

            conn = context.database.connection()
            count = conn.count(self.__model, context)

            self.__cache['count'][context] = count
            return count

    def delete(self, **context):
        pipe = self.pipe()

        # delete piped records
        if pipe:
            through = pipe.throughModel()
            q = orb.Query(pipe.target()).in_(ids)
            context['where'] = q & context.get('where')
            context = self.contextOptions(**context)
            conn = context.connection()
            return conn.delete(through, context)

        # delete normal records
        else:
            records = self.records(**context)
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
            model = self.model()
            q = orb.Query('id').in_(remove)
            context['where'] = q & context.get('where')
            context = self.contextOptions(**context)
            conn = context.connection()
            return conn.delete(model, context)

    def first(self, **context):
        context = self.contextOptions(**context)

        try:
            return self.__cache['first'][context]
        except KeyError:
            try:
                return self.__cache['records'][context][0]
            except IndexError:
                return None
            except KeyError:
                context.limit = 1
                context.order = context.order or '-id'
                records = self.records(context=context)
                record = records[0] if records else None
                self.__cache['records'][context] = record
                return record

    def hasRecord(self, record, **context):
        context = self.contextOptions(**context)
        context.returning = 'values'
        context.columns = ['id']
        return self.first(context=context) is not None

    def ids(self, **context):
        return self.values(['id'], **context)

    def index(self, record, **context):
        context = self.contextOptions(**context)
        if not record:
            return -1
        else:
            try:
                return self.__cache['records'][context].index(record)
            except KeyError:
                return self.ids().index(record.id())

    def isEmpty(self, **context):
        return self.count(**context) == 0

    def isNull(self):
        return self.__records is None and self.__model is None

    def iterate(self, batch=1):
        return CollectionIterator(self, batch)

    def last(self, **context):
        context = self.contextOptions(context)
        try:
            return self.__cache['last'][context]
        except KeyError:
            record = self.reversed().first(**context)
            self.__cache['last'][context]
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
        size = max(0, self.contextOptions(**context).pageSize)
        if not size:
            return self.copy()
        else:
            return self.copy(page=number, pageSize=size)

    def pageCount(self, **context):
        size = max(0, self.contextOptions(**context).pageSize)

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
        context = self.contextOptions(**context)
        try:
            return self.__cache['records'][context]
        except KeyError:
            conn = context.database.connection()
            raw = conn.select(self.__model, context)

            if context.inflated and context.returning != 'values':
                records = [self.__table.inflate(x) for x in raw]
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

            self.__cache['records'][context] = records
            return records

    def refine(self, **context):
        context = self.contextOptions(**context)
        other = orb.Collection(
            records=self.__cache['records'].get(context),
            model=self.__model,
            owner=self.__owner,
            source=self.__source,
            context=context
        )
        return other

    def remove(self, record, **context):
        if self.pipe():
            pass
        else:
            raise NotImplementedError

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
                q  = orb.Query(through, pipe.source()) == self.__owner
                q &= orb.Query(through, pipe.target()).in_(remove_ids)
                through.select(where=q).delete()

            # create new records
            if add_ids:
                collection = orb.Collection([through({pipe.source(): self.__owner, pipe.target(): id})
                                             for id in add_ids])
                collection.commit()

            return len(add_ids), len(remove_ids)

        else:
            raise NotImplementedError

    def setModel(self, model):
        self.__model = model

    def setPipe(self, pipe):
        self.__pipe = pipe