from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class WHERE(SQLiteStatement):
    def __call__(self, model, query):
        return '', {}


SQLiteStatement.registerAddon('WHERE', WHERE())
