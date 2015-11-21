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

    def __call__(self, record, **context):
        if not record.isRecord():
            return orb.Collection()

        model = self.toModel()
        ref = self.throughModel()

        # create the pipe query
        q  = orb.Query(model) == orb.Query(ref, self.to())
        q &= orb.Query(ref, self.from_()) == record

        context['where'] = q & context.get('where')

        # generate the pipe query for this record
        return model.select(pipe=self, record=record, **context)

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
        if not schema:
            raise orb.errors.ModelNotFound(self.__through)
        else:
            return schema.column(self.__from)

    def fromModel(self):
        col = self.fromColumn()
        return col.referenceModel()

    def to(self):
        return self.__to

    def toColumn(self):
        schema = orb.system.schema(self.__through)
        if not schema:
            raise orb.errors.ModelNotFound(self.__through)
        else:
            return schema.column(self.__to)

    def toModel(self):
        col = self.toColumn()
        if col:
            return col.referenceModel()
        else:
            return None

    def through(self):
        return self.__through

    def throughModel(self):
        schema = orb.system.schema(self.__through)
        if not schema:
            raise orb.errors.ModelNotFound(self.__through)
        else:
            return schema.model()

    def unique(self):
        return self.__unique