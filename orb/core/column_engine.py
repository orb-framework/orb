"""
Defines an engine for processing data flowing to and from
the backend.
"""

import orb

from ..utils.text import safe_eval


class ColumnEngine(object):
    MathMap = {
        'Default': {
            'Add': u'{field} + {value}',
            'Subtract': u'{field} - {value}',
            'Multiply': u'{field} * {value}',
            'Divide': u'{field} / {value}',
            'And': u'{field} & {value}',
            'Or': u'{field} | {value}'
        }
    }

    def __init__(self, type_map=None, math_map=None):
        self.__type_map = type_map or {}
        self.__math_map = math_map or {}

    def database_restore(self, column, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param typ: <str>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # restore translatable column
        if (column.test_flag(orb.Column.Flags.I18n) and
            isinstance(db_value, (str, unicode))):
            if db_value.startswith('{') and db_value.endswith('}'):
                try:
                    return safe_eval(db_value)
                except StandardError:
                    return {context.locale: db_value}
            else:
                return {context.locale: db_value}
        else:
            return db_value

    def database_math(self, column, typ, field, op, value):
        """
        Performs some database math on the given field.  This will be database specific
        implementations and should return the resulting database operation.

        :param field: <str>
        :param op: <orb.Query.Math>
        :param target: <variant>
        :param context: <orb.Context> || None

        :return: <str>
        """
        ops = orb.Query.Math(op)
        format = self.MathMap.get(typ, {}).get(ops) or self.MathMap.get('Default').get(ops) or '{field}'
        return format.format(field=field, value=value)

    def database_store(self, column, typ, py_value):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # convert base types to work in the database
        if isinstance(py_value, (list, tuple, set)):
            py_value = tuple((self.database_store(x) for x in py_value))
        elif isinstance(py_value, orb.Collection):
            py_value = py_value.ids()
        elif isinstance(py_value, orb.Model):
            py_value = py_value.id()

        return py_value

    def database_type(self, column, typ):
        """
        Returns the database object type based on the given connection type.

        :param typ:  <str>

        :return: <str>
        """
        return self.__type_map.get(typ, self.__database_types.get('default'))
