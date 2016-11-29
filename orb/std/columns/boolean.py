import projex.text
import random

from orb.core.column_engine import ColumnEngine
from orb.core.column import Column


class BooleanColumn(Column):
    """
    Defines a boolean field.  This column will return True or False, or None if no value
    has been set.  The default value for this column is None.
    """
    __default_engine__ = ColumnEngine(type_map={
        'Postgres': 'BOOLEAN',
        'SQLite': 'INTEGER',
        'MySQL': 'BOOLEAN'
    })

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return bool(random.randint(0, 1))

    def value_from_string(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value).lower() == 'true'

