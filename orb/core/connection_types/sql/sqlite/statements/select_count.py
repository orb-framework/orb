from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class SELECT_COUNT(SQLiteStatement):
    def __call__(self, model, context):
        SELECT = self.byName('SELECT')
        columns = context.columns or [model.schema().id_column().field()]
        sql, data = SELECT(model, orb.Context(columns=columns, context=context))
        if sql:
            sql = 'SELECT COUNT(*) AS count FROM ({0}) AS records;'.format(sql)
        return sql, data


SQLiteStatement.registerAddon('SELECT COUNT', SELECT_COUNT())