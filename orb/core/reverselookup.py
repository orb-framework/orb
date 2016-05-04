from projex.lazymodule import lazy_import
from .collector import Collector

orb = lazy_import('orb')

class ReverseLookup(Collector):
    def __json__(self):
        output = super(ReverseLookup, self).__json__()
        output['model'] = self.__reference
        output['target'] = self.targetColumn().field()
        return output

    def __init__(self, reference='', target='', from_column='', **options):
        if from_column:
            reference, _, target = from_column.partition('.')

        options['model'] = reference
        super(ReverseLookup, self).__init__(**options)

        # custom options
        self.__reference = reference
        self.__target = target

    def collect(self, record, **context):
        if not record.isRecord():
            return orb.Collection()
        else:
            model = self.referenceModel()

            # create the pipe query
            q  = orb.Query(model, self.__target) == record

            context['where'] = q & context.get('where')
            return model.select(record=record, collector=self, **context)

    def collectExpand(self, query, parts, **context):
        rmodel = self.referenceModel()
        sub_q = query.copy()
        sub_q._Query__column = '.'.join(parts[1:])
        sub_q._Query__model = rmodel
        return rmodel.select(columns=[self.targetColumn()], where=sub_q)

    def copy(self):
        out = super(ReverseLookup, self).copy()
        out._ReverseLookup__reference = self.__reference
        out._ReverseLookup__target = self.__target
        return out

    def referenceModel(self):
        schema = orb.system.schema(self.__reference)
        if schema is not None:
            return schema.model()
        else:
            raise orb.errors.ModelNotFound(self.__reference)

    def targetColumn(self):
        schema = orb.system.schema(self.__reference)
        try:
            return schema.column(self.__target)
        except AttributeError:
            raise orb.errors.ModelNotFound(self.__reference)

