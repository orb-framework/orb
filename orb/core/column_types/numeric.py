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
    TypeMap = {
        'Postgres': 'DECIMAL',
        'Default': 'DECIMAL UNSIGNED'
    }


class FloatColumn(AbstractNumericColumn):
    TypeMap = {
        'Postgres': 'DOUBLE PRECISION',
        'Default': 'DOUBLE UNSIGNED'
    }


class IntegerColumn(AbstractNumericColumn):
    TypeMap = {
        'Postgres': 'INTEGER',
        'Default': 'INT UNSIGNED'
    }


class LongColumn(AbstractNumericColumn):
    TypeMap = {
        'Default': 'BIGINT'
    }


class IdColumn(LongColumn):
    def __init__(self, **kwds):
        super(IdColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(self.Flags.Required)
        self.setFlag(self.Flags.Primary)
        self.setFlag(self.Flags.AutoIncrement)


Column.registerAddon('Decimal', DecimalColumn)
Column.registerAddon('Float', FloatColumn)
Column.registerAddon('Integer', IntegerColumn)
Column.registerAddon('Long', LongColumn)
Column.registerAddon('Id', IdColumn)