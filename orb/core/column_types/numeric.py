import projex.text

from ..column import Column

class AbstractNumericColumn(Column):
    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        try:
            return projex.text.safe_eval(value)
        except ValueError:
            return 0


class DecimalColumn(AbstractNumericColumn):
    pass


class FloatColumn(AbstractNumericColumn):
    pass


class IntegerColumn(AbstractNumericColumn):
    pass


class LongColumn(AbstractNumericColumn):
    pass

class SerialColumn(LongColumn):
    def __init__(self, **kwds):
        super(SerialColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(self.Flags.Primary)
        self.setFlag(self.Flags.AutoIncrement)

Column.registerAddon('Decimal', DecimalColumn)
Column.registerAddon('Float', FloatColumn)
Column.registerAddon('Integer', IntegerColumn)
Column.registerAddon('Long', LongColumn)
Column.registerAddon('Serial', SerialColumn)