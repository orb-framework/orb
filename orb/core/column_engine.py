"""
Defines an engine for processing data flowing to and from
the backend.
"""

import orb

from collections import defaultdict
from ..utils.text import safe_eval
from .query import Query


class ColumnEngine(object):
    def __init__(self, type_map=None):
        self.__type_map = type_map or {}
        self.__math_operators = defaultdict(dict)

        # populate the default operator definitions
        self.assign_operators({
            Query.Math.Add: u'+',
            Query.Math.Subtract: u'-',
            Query.Math.Multiply: u'*',
            Query.Math.Divide: u'/',
            Query.Math.And: u'&',
            Query.Math.Or: u'|'
        })

    def assign_operator(self, op, symbol, plugin_name='default'):
        """
        Assigns the symbol to the given math operator.  If no specific connection plugin
        name is specified, then this will override the default operator definition.

        :param op: <orb.Query.Math>
        :param symbol: <unicode>
        :param plugin_name: <str>
        """
        self.__math_operators[plugin_name][op] = symbol

    def assign_operators(self, operators, plugin_name='default'):
        """
        Updates the mapping between the query operators and the symbols for a particular connection
        plugin.  If no explicit plugin is provided, then the default associations will be updated.

        :param operators: {<Query.Math>: <unicode> symbol, ..}
        :param plugin_name: <str>
        """
        self.__math_operators[plugin_name].update(operators)

    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Restores data from a backend connection and converts it back
        to a Python value that is expected by the system.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant> python value
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

    def get_column_type(self, column, plugin_name):
        """
        Returns the database object type based on the given connection type.

        :param column: <str>
        :param plugin_name:  <str>

        :return: <str>
        """
        return self.__type_map.get(plugin_name, self.__type_map.get('default'))

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # convert base types to work in the database
        if isinstance(py_value, (list, tuple, set)):
            py_value = tuple((self.get_database_value(x, context=context) for x in py_value))
        elif isinstance(py_value, orb.Collection):
            py_value = py_value.ids()
        elif isinstance(py_value, orb.Model):
            py_value = py_value.id()

        return py_value

    def get_math_statment(self, column, plugin_name, field, op, value):
        """
        Generates a mathematical statement for the backend based on the plugin type
        and operator mappings associated with this engine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param field: <unicode>
        :param op: <Query.Math>
        :param value: <variant>

        :return: <str>
        """
        explicit = self.__math_operators[plugin_name]
        implicit = self.__math_operators[plugin_name]
        db_op = explicit.get(op, implicit[op])
        return u' '.join(field, db_op, value)
