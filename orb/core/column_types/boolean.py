import projex.text
import random

from ..column import Column


class BooleanColumn(Column):
    """
    Defines a boolean field.  This column will return True or False, or None if no value
    has been set.  The default value for this column is None.
    """
    TypeMap = {
        'Postgres': 'BOOLEAN',
        'SQLite': 'INTEGER',
        'MySQL': 'BOOLEAN'
    }

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return bool(random.randint(0, 1))

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value).lower() == 'true'


# register column addon
Column.registerAddon('Boolean', BooleanColumn)