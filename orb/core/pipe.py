import projex.text

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class Pipe(object):
    __lookup__ = True

    def __init__(self, name='', through='', from_='', to='', unique=False):
        self.__name = self.__name__ = name
        self.__through = through
        self.__from = from_
        self.__to = to
        self.__unique = unique
        self.__schema = None
        self.__preload = None

    def __call__(self, record, **context):
        if not record.isRecord():
            return orb.Collection()

        target = self.toModel()
        through = self.throughModel()

        # create the pipe query
        q  = orb.Query(target) == orb.Query(through, self.to())
        q &= orb.Query(through, self.from_()) == record

        context['where'] = q & context.get('where')
        cache = record.preload(projex.text.underscore(self.name()))

        # generate the pipe query for this record
        collection = target.select(pipe=self, record=record, **context)
        collection.preload(cache, **context)
        return collection

    def name(self):
        return self.__name

    def schema(self):
        return self.__schema

    def setName(self, name):
        self.__name = self.__name__ = name

    def setSchema(self, schema):
        self.__schema = schema

    def setUnique(self, state):
        self.__unique = state

    def from_(self):
        return self.__from

    def fromColumn(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.column(self.__from)
        except AttributeError:
            raise orb.errors.ModelNotFound(self.__through)

    def fromModel(self):
        col = self.fromColumn()
        return col.referenceModel() if col else None

    def to(self):
        return self.__to

    def toColumn(self):
        schema = orb.system.schema(self.__through)
        try:
            return schema.column(self.__to)
        except AttributeError:
            raise orb.errors.ModelNotFound(self.__through)

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
            raise orb.errors.ModelNotFound(self.__through)

    def unique(self):
        return self.__unique