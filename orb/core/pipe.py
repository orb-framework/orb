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
        output['model'] = self.toColumn().referenceModel().schema().name()
        return output

    def __init__(self, through_path='', through='', from_='', to='', **options):
        super(Pipe, self).__init__(**options)

        if through_path:
            through, from_, to = through_path.split('.')

        self.__through = through
        self.__from = from_
        self.__to = to

    def collect(self, record, **context):
        if not record.isRecord():
            return orb.Collection()
        else:
            target = self.toModel()
            through = self.throughModel()

            # create the pipe query
            q  = orb.Query(target) == orb.Query(through, self.to())
            q &= orb.Query(through, self.from_()) == record

            context['where'] = q & context.get('where')

            # generate the pipe query for this record
            return target.select(collector=self, record=record, **context)

    def collectExpand(self, query, parts, **context):
        through = self.throughModel()
        toModel = self.toModel()

        sub_q = query.copy()
        sub_q._Query__column = '.'.join(parts[1:])
        sub_q._Query__model = toModel
        to_records = toModel.select(columns=[toModel.schema().idColumn()], where=sub_q)
        pipe_q = orb.Query(through, self.to()).in_(to_records)
        return through.select(columns=[self.from_()], where=pipe_q)

    def copy(self):
        out = super(Pipe, self).copy()
        out._Pipe__through = self.__through
        out._Pipe__from = self.__from
        out._Pipe__to = self.__to
        return out

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
        return col.referenceModel() if col else None

    def model(self):
        return self.toModel()

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
        return col.referenceModel() if col else None

    def through(self):
        return self.__through

    def throughModel(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.model()
        except AttributeError:
            raise orb.errors.ModelNotFound(schema=self.__through)
