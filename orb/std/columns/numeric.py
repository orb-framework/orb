import demandimport

from orb.core.column import Column

with demandimport.enabled():
    import orb

INT_32 = (2**31) - 1
INT_64 = (2**63) - 1


class AbstractNumericColumn(Column):
    def __init__(self, minimum=None, maximum=None, **kwds):
        super(AbstractNumericColumn, self).__init__(**kwds)

        # used to determine ranging options
        self.__minimum = minimum
        self.__maximum = maximum

    def copy(self, **kw):
        """
        Re-implemented `copy` method from `Column`

        This will include the default minimum/maximum values
        for this column.

        Args:
            **kw:

        Returns:

        """
        kw.setdefault('minimum', self.__minimum)
        kw.setdefault('maximum', self.__maximum)
        return super(AbstractNumericColumn, self).copy(**kw)

    def maximum(self):
        """
        Returns the minimum bound for this column (if any).

        Returns:
            <int> or <float> or None

        """
        return self.__maximum

    def minimum(self):
        """
        Returns the maximum bound for this column (if any).

        Returns:
            <int> or <float> or None

        """
        return self.__minimum

    def set_maximum(self, maximum):
        """
        Sets the minimum bound for this column (if any).

        Args:
            maximum: <int> or <float> or None

        """
        self.__maximum = maximum

    def set_minimum(self, minimum):
        """
        Sets the minimum bound for this column (if any).

        Args:
            minimum: <int> or <float> or None

        """
        self.__minimum = minimum

    def validate(self, value):
        """
        Re-implements `validate` method from `Column`.

        Ensures that the given value is between the minimum and
        maximum values (if defined) for this column.

        Args:
            value: <int> or <float> or None

        Raises:
            <orb.errors.ValueOutOfRange> if the given value is incorrect

        Returns:
            <bool>

        """
        if value is not None:
            minimum_ok = self.__minimum is None or value >= self.__minimum
            maximum_ok = self.__maximum is None or value <= self.__maximum
        else:
            minimum_ok = True
            maximum_ok = True

        if not (minimum_ok and maximum_ok):
            raise orb.errors.ValueOutOfRange(self.name(), value, self.__minimum, self.__maximum)
        else:
            return super(AbstractNumericColumn, self).validate(value)

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        Args:
            value: <str> or <unicode>
            context: <orb.Context>

        Raises:
            <orb.errors.ColumnValidationError> if the value cannot be parsed

        Returns:
            <int> or <float>
        """
        if value in (None, ''):
            return value

        try:
            return float(value)
        except ValueError:
            raise orb.errors.ColumnValidationError(self, 'Invalid value to parse')


class DecimalColumn(AbstractNumericColumn):
    def __init__(self, precision=65, scale=30, **kwds):
        super(DecimalColumn, self).__init__(**kwds)

        # define custom properties
        self.__precision = precision
        self.__scale = scale

    def copy(self, **kw):
        """
        Re-implements the `copy` method from `AbstractNumericColumn`.

        This method will include the precision and scale values for this column.

        Args:
            **kw: <dict>

        Returns:
            <orb.DecimalColumn>

        """
        kw.setdefault('precision', self.__precision)
        kw.setdefault('scale', self.__scale)
        return super(DecimalColumn, self).copy(**kw)

    def precision(self):
        """
        Returns the maximum number of digits (the precision). It has a range of 1 to 65.

        Returns:
            <int>
        """
        return self.__precision

    def scale(self):
        """
        Returns the number of digits to the right of the decimal point (the scale).
        It has a range of 0 to 30 and must be no larger than the precision.

        Returns:
            <int>
        """
        return self.__scale

    def set_precision(self, precision):
        """
        Sets the maximum number of digits (the precision). It has a range of 1 to 65.

        Args:
            precision: <int>
        """
        self.__precision = precision

    def set_scale(self, scale):
        """
        Sets the number of digits to the right of the decimal point (the scale).
        It has a range of 0 to 30 and must be no larger than the precision.

        Args:
            scale: <int>
        """
        self.__scale = scale


class FloatColumn(AbstractNumericColumn):
    """ Column placeholder for floating point numbers """
    pass


class IntegerColumn(AbstractNumericColumn):
    """ 32 bit integer Field """
    def __init__(self, **kw):
        kw.setdefault('minimum', -INT_32)
        kw.setdefault('maximum', INT_32)

        super(IntegerColumn, self).__init__(**kw)

    def restore(self, value, context=None):
        """
        Re-implements the `restore` method.

        Ensures that the returned value is a long.

        Args:
            value: <variant>
            context: <orb.Context> or None

        Returns:
            <long>

        """
        value = super(IntegerColumn, self).restore(value, context=context)
        if type(value) == float:
            return int(value)
        else:
            return value

    def store(self, value, context=None):
        """
        Re-implements the `store` method.

        Ensures that the returned value is a long.

        Args:
            value: <variant>
            context: <orb.Context> or None

        Returns:
            <long>

        """
        value = super(IntegerColumn, self).store(value, context=context)
        if type(value) == float:
            return int(value)
        else:
            return value


class LongColumn(IntegerColumn):
    """ 64 bit integer field """
    def __init__(self, **kw):
        int_64 = (2**63) - 1

        kw.setdefault('minimum', -INT_64)
        kw.setdefault('maximum', INT_64)

        super(LongColumn, self).__init__(**kw)


class EnumColumn(LongColumn):
    def __init__(self, enum=None, **kwds):
        super(EnumColumn, self).__init__(**kwds)

        # define custom properties
        self.__enum = enum

    def copy(self, **kw):
        """
        Re-implements `copy` from `AbstractNumericColumn`.

        Override the copy method to include the enum by default.

        Args:
            **kw: <dict>

        Returns:
            <orb.EnumColumn>

        """
        kw.setdefault('enum', self.__enum)
        return super(EnumColumn, self).copy(**kw)

    def enum(self):
        """
        Returns the enumeration that is associated with this column.  This can
        help for automated validation when dealing with enumeration types.

        Returns:
            <orb.utils.enum.enum> or None
        """
        return self.__enum

    def set_enum(self, enum):
        """
        Sets the enumeration that is associated with this column to the inputted
        type.  This is an optional parameter but can be useful when dealing
        with validation and some of the automated features of the ORB system.

        Args:
            enum: <orb.utils.enum.enum> or None
        """
        self.__enum = enum


class IdColumn(LongColumn):
    def __init__(self, **kwds):
        super(IdColumn, self).__init__(**kwds)

        # setup flags
        self.set_flag(self.Flags.Required)
        self.set_flag(self.Flags.Unique)
        self.set_flag(self.Flags.AutoIncrement)

