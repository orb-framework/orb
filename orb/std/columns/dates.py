import datetime
import dateutil.parser
import demandimport
import logging
import time

from ...utils.timezones import (localize_timezone, utc_timezone)
from orb.core.column import Column

with demandimport.enabled():
    import orb
    import pytz


log = logging.getLogger(__name__)



class AbstractDatetimeColumn(Column):
    def __init__(self, default_format='%Y-%m-%d %H:%M:%S', **kwds):
        super(AbstractDatetimeColumn, self).__init__(**kwds)

        # define custom properties
        self.__default_format = default_format

    def default_format(self):
        """
        Returns the default format for this datetime column.

        :return: <str>
        """
        return self.__default_format

    def restore(self, value, context=None):
        """
        Re-implements the `orb.Column.restore` method to extract
        string values.

        Args:
            value: <variant>
            context: <orb.Context>

        Returns:
            <variant>
        """
        if isinstance(value, (str, unicode)):
            return self.value_from_string(value, context=context)
        else:
            return super(AbstractDatetimeColumn, self).restore(value, context=context)

    def set_default_format(self, form):
        """
        Sets the default string format for rendering this instance.

        :param form: <str>
        """
        self.__default_format = form

    def value_from_string(self, value, context=None):
        """
        Re-implements the
        Args:
            value: <variant>
            context: <orb.Context>

        Returns:
            <datetime.datetime>

        """
        if value is None:
            return None
        else:
            try:
                return self.keyword_value(value, context=context)
            except KeyError:
                try:
                    return dateutil.parser.parse(value)
                except Exception:
                    raise orb.errors.ColumnValidationError(self, 'Failed to parse date/time')

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class DateColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d')

        super(DateColumn, self).__init__(**kwds)

        # define additional keywords for a date
        self.add_keyword('today', lambda k, ctxt: datetime.date.today())
        self.add_keyword('now', lambda k, ctxt: datetime.date.today())

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        value = super(DateColumn, self).value_from_string(value, context=None)

        if type(value) == datetime.datetime:
            return value.date()
        else:
            return value


class DatetimeColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d %H:%M:%S')

        super(DatetimeColumn, self).__init__(**kwds)

        # define additional keywords for this column
        self.add_keyword('today', lambda k, ctxt: datetime.datetime.now())
        self.add_keyword('now', lambda k, ctxt: datetime.datetime.now())


class DatetimeWithTimezoneColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d %H:%M:%S')

        super(DatetimeWithTimezoneColumn, self).__init__(**kwds)

        # define additional keywords
        self.add_keyword('today', lambda k, ctxt: datetime.datetime.now())
        self.add_keyword('now', lambda k, ctxt: datetime.datetime.now())

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        value = super(DatetimeWithTimezoneColumn, self).restore(value, context)

        if isinstance(value, datetime.datetime):
            context = context or orb.Context()
            value = localize_timezone(value, pytz.timezone(context.timezone))

        return value

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, (str, unicode)):
            try:
                value = dateutil.parser.parse(value)
            except Exception:
                raise orb.errors.ColumnValidationError(self, 'Invalid date/time format')
            else:
                value = utc_timezone(value)

        elif isinstance(value, datetime.datetime):
            value = utc_timezone(value)

        return super(DatetimeWithTimezoneColumn, self).store(value, context=context)


class IntervalColumn(AbstractDatetimeColumn):
    def value_from_string(self, value, context=None):
        """
        Re-implements the `value_from_string` method from `orb.Column`

        Converts the inputted string text to a value that matches the type from
        this column type.

        Args:
            value: <str> milliseconds
            context: <orb.Context> or None

        Raises:
            <orb.errors.ColumnValidationError> if the value cannot be converted to a float

        Returns:
            <datetime.timedelta> or None
        """
        if value is None:
            return value
        else:
            try:
                milliseconds = int(value)
            except Exception:
                raise orb.errors.ColumnValidationError(self, 'Could restore interval')
            else:
                return datetime.timedelta(seconds=milliseconds / 1000.0)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        Args:
            value: <datetime.timedelta> interval or <int> milliseconds or <float> milliseconds

        Raises:
            <orb.errors.ColumnValidationError> if the value is invalid

        Returns:
            <str>
        """
        if isinstance(value, datetime.timedelta):
            return str(int(value.total_seconds() * 1000))
        elif type(value) in (int, float):
            return str(int(value))
        elif value is None:
            return None
        else:
            raise orb.errors.ColumnValidationError(self, 'Invalid store interval')


class TimeColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%H:%M:%S')

        super(TimeColumn, self).__init__(**kwds)

        # register additional keywords
        self.add_keyword('now', lambda k, ctxt: datetime.datetime.now().time())

    def restore(self, value, context=None):
        """
        Converts a stored database value to Python.

        Args:
            value: <datetime.timedelta> or <datetime.time> or <str>
            context: <orb.Context>

        Returns:
            <datetime.time>
        """
        if isinstance(value, datetime.timedelta):
            hours, remain = divmod(value.seconds, 3600)
            minutes, seconds = divmod(remain, 60)
            return datetime.time(hours, minutes, seconds)
        else:
            return super(TimeColumn, self).restore(value, context=context)

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        Args:
            value: <str>
            context: <orb.Context>

        Returns:

        """
        value = super(TimeColumn, self).value_from_string(value, context=context)
        if type(value) == datetime.datetime:
            return value.time()
        else:
            return value


class TimestampColumn(AbstractDatetimeColumn):
    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param value: <variant>
        :param context: <orb.Context> or None

        :return: <datetime>
        """
        if isinstance(value, (int, long, float)):
            return datetime.datetime.fromtimestamp(value)
        else:
            return super(TimestampColumn, self).restore(value, context=context)

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param value: <variant>
        :param context: <orb.Context> or None

        :return: <variant>
        """
        if isinstance(value, datetime.datetime):
            return time.mktime(value.timetuple())
        else:
            return super(TimestampColumn, self).store(value, context=context)

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        try:
            unix_time = float(value)
        except ValueError:
            return super(TimestampColumn, self).value_from_string(value, context=context)
        else:
            return datetime.datetime.fromtimestamp(unix_time)

