import projex.text

from projex.lazymodule import lazy_import
from ..column import Column

orb = lazy_import('orb')

class AbstractNumericColumn(Column):
    def __init__(self, minimum=None, maximum=None, **kwds):
        super(AbstractNumericColumn, self).__init__(**kwds)

        # used to determine ranging options
        self.__minimum = minimum
        self.__maximum = maximum

    def maximum(self):
        return self.__maximum

    def minimum(self):
        return self.__minimum

    def setMaximum(self, maximum):
        self.__maximum = maximum

    def setMinimum(self, minimum):
        self.__minimum = minimum

    def validate(self, value):
        if (value is not None and
            (self.__minimum is not None and value < self.__minimum or
             self.__maximum is not None and value > self.__maximum)):
            raise orb.errors.ValueOutOfRange(self.name(), value, self.__minimum, self.__maximum)
        else:
            return super(AbstractNumericColumn, self).validate(value)

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

    def __init__(self, minimum=None, maximum=None, **kwds):
        if minimum is None:
            minimum = -(2**31)
        if maximum is None:
            maximum = (2**31) - 1

        super(IntegerColumn, self).__init__(minimum=minimum,
                                            maximum=maximum,
                                            **kwds)


class LongColumn(AbstractNumericColumn):
    TypeMap = {
        'Default': 'BIGINT'
    }

    def __init__(self, minimum=None, maximum=None, **kwds):
        if minimum is None:
            minimum = -(2**63)
        if maximum is None:
            maximum = (2**63) - 1

        super(LongColumn, self).__init__(minimum=minimum,
                                         maximum=maximum,
                                         **kwds)


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


class EnumColumn(LongColumn):
    def __init__(self, enum=None, **kwds):
        super(EnumColumn, self).__init__(**kwds)

        # define custom properties
        self.__enum = enum

    def enum(self):
        """
        Returns the enumeration that is associated with this column.  This can
        help for automated validation when dealing with enumeration types.

        :return     <projex.enum.enum> || None
        """
        return self.__enum

    def setEnum(self, cls):
        """
        Sets the enumeration that is associated with this column to the inputted
        type.  This is an optional parameter but can be useful when dealing
        with validation and some of the automated features of the ORB system.

        :param      cls | <projex.enum.enum> || None
        """
        self.__enum = cls


Column.registerAddon('Enum', EnumColumn)
Column.registerAddon('Decimal', DecimalColumn)
Column.registerAddon('Float', FloatColumn)
Column.registerAddon('Integer', IntegerColumn)
Column.registerAddon('Long', LongColumn)
Column.registerAddon('Id', IdColumn)