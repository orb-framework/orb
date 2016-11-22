import os

from projex.lazymodule import lazy_import
from orb.core.column import Column
from orb.core.column_engine import ColumnEngine

orb = lazy_import('orb')

class IdColumnEngine(ColumnEngine):
    NumericTypes = {
        'Postgres': 'SERIAL',
        'SQLite': 'INTEGER',
        'default': 'BIGINT AUTO_INCREMENT'
    }

    HashTypes = {
        'Postgres': 'character varying({bits})',
        'SQLite': 'TEXT',
        'default': 'varchar({bits})'
    }

    DefaultStore = {
        'Postgres': 'DEFAULT',
        'MySQL': 'DEFAULT'
    }

    def get_column_type(self, column, plugin_name):
        """
        Re-implements the `orb.ColumnEngine.get_column_type` method.

        Returns the database representation of the given `IdColumn` for
        a particular database type.

        :param column: <orb.Column>
        :param plugin_name: <str>

        :return: <str>
        """
        if column.type() in {'default', 'numeric'}:
            return self.NumericTypes.get(plugin_name, self.NumericTypes['default'])
        elif column.type() == 'hash':
            base = self.HashTypes.get(plugin_name, self.HashTypes['default'])
            return base.format(bits=column.bits() * 2)
        else:
            raise orb.errors.OrbError('Unknown ID type: {0}'.format(column.type()))

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Re-implements the `orb.ColumnEngine.get_database_value` method.

        This method will take a Python value and prepre it for saving to
        the database.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant> database value
        """
        if py_value is not None:
            return py_value
        elif column.type() == 'hash':
            return os.urandom(column.bits()).encode('hex')
        else:
            return self.DefaultStore.get(plugin_name, py_value)


class IdColumn(Column):
    __default_engine__ = IdColumnEngine()

    def __init__(self, type='default', bits=32, **kwds):
        super(IdColumn, self).__init__(**kwds)

        # common to all ID columns
        self.set_flag(self.Flags.Required)
        self.set_flag(self.Flags.Unique)

        # common to all default IDs
        if type != 'hash':
            self.set_flag(self.Flags.AutoAssign)

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

    def type(self):
        return self.__type

    def validate(self, value):
        if value is None:
            # none types will be auto generated in the database
            return True
        else:
            return super(IdColumn, self).validate(value)