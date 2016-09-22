from projex.lazymodule import lazy_import
from ..sqliteconnection import SQLiteStatement

orb = lazy_import('orb')


class ADD_COLUMN(SQLiteStatement):
    def __call__(self, column):
        # determine all the flags for this column
        flags = []
        Flags = orb.Column.Flags
        for key, value in Flags.items():
            # SQLite has an issue with adding NOT NULL constraints during an ADD COLUMN during
            # modification of an existing table
            if value == Flags.Required:
                continue

            # otherwise, add the column flag
            elif column.flags() & value:
                flag_sql = SQLiteStatement.byName('Flag::{0}'.format(key))
                if flag_sql:
                    flags.append(flag_sql)

        sql = u'ADD COLUMN `{0}` {1} {2}'.format(column.field(), column.dbType('SQLite'), ' '.join(flags)).strip()
        return sql, {}


SQLiteStatement.registerAddon('ADD COLUMN', ADD_COLUMN())
