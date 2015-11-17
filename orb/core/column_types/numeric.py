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
    TypeMap = {
        'Postgres': 'SERIAL',
        'Default': 'BIGINT'
    }
    def __init__(self, **kwds):
        super(IdColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(self.Flags.Required)
        self.setFlag(self.Flags.Primary)
        self.setFlag(self.Flags.AutoIncrement)

    def dbStore(self, typ, py_value, context=None):
        if py_value is None:
            if typ == 'Postgres':
                return 'DEFAULT'
            else:
                return py_value
        else:
            return py_value


Column.registerAddon('Decimal', DecimalColumn)
Column.registerAddon('Float', FloatColumn)
Column.registerAddon('Integer', IntegerColumn)
Column.registerAddon('Long', LongColumn)
Column.registerAddon('Id', IdColumn)