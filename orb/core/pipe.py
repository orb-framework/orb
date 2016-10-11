import projex.text

from projex.lazymodule import lazy_import
from .collector import Collector

orb = lazy_import('orb')


class Pipe(Collector):
    def __json__(self):
        output = super(Pipe, self).__json__()
        output['through'] = self.through()
        output['from'] = self.fromColumn().field()
        output['to'] = self.toColumn().field()
        output['model'] = self.toColumn().reference_model().schema().name()
        return output

    def __init__(self, through_path='', through='', from_='', to='', **options):
        super(Pipe, self).__init__(**options)

        if through_path:
            through, from_, to = through_path.split('.')

        self.__through = through
        self.__from = from_
        self.__to = to

    def add_record(self, source_record, target_record, **context):
        through_model = self.throughModel()
        data = {
            self.from_(): source_record,
            self.to(): target_record
        }
        return through_model.ensureExists(data, **context)

    def create_record(self, source_record, values, **context):
        target_model = self.toModel()
        target_col = self.toColumn()
        orb_context = orb.Context(**context)

        # add the target based on the name or field
        if target_col.name() in values or target_col.field() in values:
            target_record = values.get(target_col.name(), values.get(target_col.field()))
            if not isinstance(target_record, orb.Model):
                target_record = target_model(target_record,
                                             context=orb_context)

        # otherwise, create the target based on the given values
        else:
            target_record = target_model(values, context=orb_context)

        self.add_record(source_record, target_record)
        return target_record

    def collect(self, record, **context):
        if not record.is_record():
            return orb.Collection()
        else:
            target = self.toModel()
            through = self.throughModel()

            # create the pipe query
            q  = orb.Query(target) == orb.Query(through, self.to())
            q &= orb.Query(through, self.from_()) == record

            context['where'] = q & context.get('where')

            # generate the pipe query for this record
            records = target.select(**context)
            return records.bind_collector(self).bind_source_record(record)

    def collect_expand(self, query, parts, **context):
        through = self.throughModel()
        toModel = self.toModel()

        sub_q = query.copy()
        sub_q._Query__column = '.'.join(parts[1:])
        sub_q._Query__model = toModel
        to_records = toModel.select(columns=[toModel.schema().id_column()], where=sub_q)
        pipe_q = orb.Query(through, self.to()).in_(to_records)
        return through.select(columns=[self.from_()], where=pipe_q)

    def copy(self):
        out = super(Pipe, self).copy()
        out._Pipe__through = self.__through
        out._Pipe__from = self.__from
        out._Pipe__to = self.__to
        return out

    def delete_records(self, collection, **context):
        through = self.throughModel()

        # remove them from the system
        orb_context = orb.Context(**context)

        q = orb.Query(self.targetColumn()).in_(collection)
        context['where'] = q & context.get('where')

        # lookup the records that are about to be removed
        # in order to run their deletion event
        records = through.select(**context)

        delete_records = list(orb.events.DeleteEvent.process(records))

        # delete the records from the connection
        if delete_records:
            conn = orb_context.db.connection()
            count = conn.delete(delete_records, orb_context)[1]
        else:
            count = 0
        return True, count

    def from_(self):
        return self.__from

    def fromColumn(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.column(self.__from)
        except AttributeError:
            raise orb.errors.ModelNotFound(schema=self.__through)

    def fromModel(self):
        col = self.fromColumn()
        return col.reference_model() if col else None

    def model(self):
        return self.toModel()

    def remove_record(self, source_record, target_record, **context):
        through = self.throughModel()
        q = orb.Query(self.from_()) == source_record
        q &= orb.Query(self.to()) == target_record

        context['where'] = q

        orb_context = orb.Context(**context)
        records = through.select(context=orb_context)
        delete_records = orb.events.DeleteEvent.process(records)

        if delete_records:
            conn = orb_context.db.connection()
            conn.delete(delete_records, orb_context)
            return len(delete_records)
        else:
            return 0

    def to(self):
        return self.__to

    def toColumn(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.column(self.__to)
        except AttributeError:
            raise orb.errors.ModelNotFound(schema=self.__through)

    def toModel(self):
        col = self.toColumn()
        return col.reference_model() if col else None

    def through(self):
        return self.__through

    def throughModel(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.model()
        except AttributeError:
            raise orb.errors.ModelNotFound(schema=self.__through)

    def update_records(self, source_record, collection, collection_ids, **context):
        orb_context = orb.Context(**context)

        through = self.throughModel()
        curr_ids = set(collection.ids())
        new_ids = set(collection_ids)

        # calculate the id shift
        remove_ids = curr_ids - new_ids
        add_ids = new_ids - curr_ids

        from_column = self.from_()
        to_column = self.to()

        # remove old records
        if remove_ids:
            q = orb.Query(through, from_column) == source_record
            q &= orb.Query(through, to_column).in_(remove_ids)
            orb_context.where = q
            through.select(context=orb_context).delete()

        # create new records
        if add_ids:
            collection = orb.Collection([
                through({
                    from_column: source_record,
                    to_column: id_
                }, context=orb_context)
                for id_ in add_ids
            ])
            collection.save()