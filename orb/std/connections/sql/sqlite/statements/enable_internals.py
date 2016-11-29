from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class ENABLE_INTERNALS(SQLiteStatement):
    def __call__(self, model=None, enabled=True):
        if model is not None:
            sql = u'ALTER TABLE `{0}` {1} KEYS;'.format(model.schema().dbname(), 'ENABLE' if enabled else 'DISABLE')
        else:
            sql = u'PRAGMA foreign_keys={0};' \
                  u'PRAGMA count_changes={0};'.format('TRUE' if enabled else 'FALSE')

        return sql, {}


SQLiteStatement.registerAddon('ENABLE INTERNALS', ENABLE_INTERNALS())
