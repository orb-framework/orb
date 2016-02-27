from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class ADD_COLUMN(PSQLStatement):
    def __call__(self, column):
        # determine all the flags for this column
        flags = []
        Flags = orb.Column.Flags
        for key, value in Flags.items():
            if column.flags() & value:
                flag_sql = PSQLStatement.byName('Flag::{0}'.format(key))
                if flag_sql:
                    flags.append(flag_sql)

        sql = u'ADD COLUMN "{0}" {1} {2}'.format(column.field(), column.dbType('Postgres'), ' '.join(flags)).strip()
        return sql, {}


PSQLStatement.registerAddon('ADD COLUMN', ADD_COLUMN())
