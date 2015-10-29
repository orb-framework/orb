import projex.text

from ..column import Column


class BooleanColumn(Column):
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