from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class SELECT_COUNT(PSQLStatement):
    def __call__(self, model, context):
        SELECT = self.byName('SELECT')
        columns = context.columns or [model.schema().idColumn().field()]
        sql, data = SELECT(model, orb.Context(columns=columns, context=context))
        if sql:
            sql = 'SELECT COUNT(*) AS count FROM ({0}) AS records;'.format(sql)
        return sql, data


PSQLStatement.registerAddon('SELECT COUNT', SELECT_COUNT())