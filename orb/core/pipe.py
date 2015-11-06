from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class Pipe(object):
    def __init__(self, name='', through='', source='', target='', unique=False):
        self.__name = self.__name__ = name
        self.__through = through
        self.__source = source
        self.__target = target
        self.__unique = unique

    def __call__(self, record, **context):
        if not record.isRecord():
            return orb.Collection()

        model = self.targetModel()
        ref = self.throughModel()

        # create the pipe query
        q  = orb.Query(model) == orb.Query(ref, self.target())
        q &= orb.Query(ref, self.source()) == record

        context['where'] = q & context.get('where')

        # generate the pipe query for this record
        records = model.select(**context)
        records.setPipe(self)
        return records

    def name(self):
        return self.__name

    def setUnique(self, state):
        self.__unique = state

    def source(self):
        return self.__source

    def sourceColumn(self):
        schema = orb.system.schema(self.__through)
        if not schema:
            raise orb.errors.ModelNotFound(self.__through)
        else:
            return schema.column(self.__source)

    def target(self):
        return self.__target

    def targetColumn(self):
        schema = orb.system.schema(self.__through)
        if not schema:
            raise orb.errors.ModelNotFound(self.__through)
        else:
            return schema.column(self.__target)

    def targetModel(self):
        col = self.targetColumn()
        if col:
            return col.throughModel()
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