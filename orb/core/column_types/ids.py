import os

from projex.lazymodule import lazy_import
from ..column import Column

orb = lazy_import('orb')

class IdColumn(Column):
    def __init__(self, type='default', bits=32, **kwds):
        super(IdColumn, self).__init__(**kwds)

        # common to all ID columns
        self.setFlag(self.Flags.Required)
        self.setFlag(self.Flags.Unique)

        # common to all default IDs
        if type != 'hash':
            self.setFlag(self.Flags.AutoAssign)

        # set default properties
        self.__type = type
        self.__bits = bits

    def bits(self):
        return self.__bits

    def copy(self):
        out = super(IdColumn, self).copy()
        out._IdColumn__type = self.__type
        out._IdColumn__bits = self.__bits
        return out

    def dbStore(self, typ, py_value, context=None):
        if py_value is None:
            if self.type() in {'default', 'numeric'}:
                if typ in ('Postgres', 'MySQL'):
                    return 'DEFAULT'
                else:
                    return py_value
            elif self.type() == 'hash':
                return os.urandom(self.__bits).encode('hex')
            else:
                raise orb.errors.OrbError('Invalid ID type: {0}'.format(self.__type))
        else:
            return py_value

    def dbType(self, typ):
        if self.type() in {'default', 'numeric'}:
            if typ == 'Postgres':
                return 'SERIAL'
            elif typ == 'SQLite':
                return 'INTEGER'
            else:
                return 'BIGINT AUTO_INCREMENT'

        elif self.type() == 'hash':
            if typ == 'Postgres':
                return 'character varying({0})'.format(self.__bits * 2)
            elif typ == 'SQLite':
                return 'TEXT'
            else:
                return 'varchar({0})'.format(self.__bits * 2)

        else:
            raise orb.errors.OrbError('Unknown ID type: {0}'.format(self.__type))

    def type(self):
        return self.__type

    def validate(self, value):
        if value is None:
            # none types will be auto generated in the database
            return True
        else:
            return super(IdColumn, self).validate(value)