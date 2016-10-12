import demandimport
import projex.text
import random

from ..column import Column
from ..column_engine import ColumnEngine

with demandimport.enabled():
    import orb


class DecimalColumnEngine(ColumnEngine):
    def get_column_type(self, column, plugin_name):
        """
        Re-implements the get_column_type method from `orb.ColumnEngine`.

        This method will return the database type based on the given decimal column.

        :param column: <orb.DecimalColumn>
        """
        db_type = super(DecimalColumnEngine, self).get_column_type(column, plugin_name)
        return db_type.format(precision=column.precision(), scale=column.scale())


class AbstractNumericColumn(Column):
    def __init__(self, minimum=None, maximum=None, **kwds):
        super(AbstractNumericColumn, self).__init__(**kwds)

        # used to determine ranging options
        self.__minimum = minimum
        self.__maximum = maximum

    def copy(self):
        out = super(AbstractNumericColumn, self).copy()
        out.setMinimum(self.__minimum)
        out.setMaximum(self.__maximum)
        return out

    def maximum(self):
        return self.__maximum

    def minimum(self):
        return self.__minimum

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        minimum = self.minimum() or 0
        maximum = self.maximum() or 100
        return random.randint(minimum, maximum)

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

    def value_from_string(self, value, extra=None, db=None):
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
    __default_engine__ = DecimalColumnEngine(type_map={
        'Postgres': u'DECIMAL({precision}, {scale})',
        'SQLite': u'REAL',
        'MySQL': u'DECIMAL({precision}, {scale})'
    })

    def __init__(self, precision=65, scale=30, **kwds):
        super(DecimalColumn, self).__init__(**kwds)

        # define custom properties
        self.__precision = precision
        self.__scale = scale

    def precision(self):
        """
        Returns the maximum number of digits (the precision). It has a range of 1 to 65.

        :return: <int>
        """
        return self.__precision

    def scale(self):
        """
        Returns the number of digits to the right of the decimal point (the scale).
        It has a range of 0 to 30 and must be no larger than the precision.

        :return: <int>
        """
        return self.__scale

    def setPrecision(self, precision):
        """
        Sets the maximum number of digits (the precision). It has a range of 1 to 65.

        :param precision: <int>
        """
        self.__precision = precision

    def setScale(self, scale):
        """
        Sets the number of digits to the right of the decimal point (the scale).
        It has a range of 0 to 30 and must be no larger than the precision.

        :param scale: <int>
        """
        self.__scale = scale


class FloatColumn(AbstractNumericColumn):
    __default_engine__ = ColumnEngine(type_map={
        'Postgres': 'DOUBLE PRECISION',
        'SQLite': 'REAL',
        'MySQL': 'DOUBLE'
    })


class IntegerColumn(AbstractNumericColumn):
    __default_engine__ = ColumnEngine(type_map={
        'Postgres': 'INTEGER',
        'SQLite': 'INTEGER',
        'MySQL': 'INTEGER'
    })

    def __init__(self, minimum=None, maximum=None, **kwds):
        if minimum is None:
            minimum = -(2**31)
        if maximum is None:
            maximum = (2**31) - 1

        super(IntegerColumn, self).__init__(minimum=minimum,
                                            maximum=maximum,
                                            **kwds)


class LongColumn(AbstractNumericColumn):
    __default_engine__ = ColumnEngine(type_map={
        'Postgres': 'BIGINT',
        'SQLite': 'INTEGER',
        'MySQL': 'BIGINT'
    })

    def __init__(self, minimum=None, maximum=None, **kwds):
        if minimum is None:
            minimum = -(2**63)
        if maximum is None:
            maximum = (2**63) - 1

        super(LongColumn, self).__init__(minimum=minimum,
                                         maximum=maximum,
                                         **kwds)


class EnumColumn(LongColumn):
    def __init__(self, enum=None, **kwds):
        super(EnumColumn, self).__init__(**kwds)

        # define custom properties
        self.__enum = enum

    def copy(self):
        out = super(EnumColumn, self).copy()
        out.setEnum(self.__enum)
        return out

    def enum(self):
        """
        Returns the enumeration that is associated with this column.  This can
        help for automated validation when dealing with enumeration types.

        :return: <orb.utils.enum.enum> or None
        """
        return self.__enum

    def setEnum(self, cls):
        """
        Sets the enumeration that is associated with this column to the inputted
        type.  This is an optional parameter but can be useful when dealing
        with validation and some of the automated features of the ORB system.

        :param cls: <orb.utils.enum.enum> or None
        """
        self.__enum = cls

