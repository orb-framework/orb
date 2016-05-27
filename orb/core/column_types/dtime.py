import datetime
import logging
import time

from projex.lazymodule import lazy_import
from ..column import Column

# optional imports
try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None


log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')


class AbstractDatetimeColumn(Column):

    def __init__(self, defaultFormat='%Y-%m-%d %H:%M:%S', **kwds):
        super(AbstractDatetimeColumn, self).__init__(**kwds)

        self.__defaultFormat = defaultFormat

    def defaultFormat(self):
        """
        Returns the default format for this datetime column.

        :return: <str>
        """
        return self.__defaultFormat

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        utc_now = datetime.datetime.utcnow()
        return self.valueFromString(utc_now.strftime(self.defaultFormat()))

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if db_value is None:
            return None
        elif isinstance(db_value, (str, unicode)):
            return self.valueFromString(db_value, context=context)
        else:
            return super(AbstractDatetimeColumn, self).dbRestore(db_value, context=context)

    def dbStore(self, typ, py_value, context=None):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if py_value is None:
            return None
        return self.valueToString(py_value, context=context)

    def setDefaultFormat(self, form):
        """
        Sets the default string format for rendering this instance.

        :param form: <str>
        """
        self.__defaultFormat = form


class DateColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'DATE',
        'SQLite': 'TEXT',
        'MySQL': 'DATE'
    }

    def __init__(self, **kwds):
        kwds.setdefault('defaultFormat', '%Y-%m-%d')

        super(DateColumn, self).__init__(**kwds)

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        return db_value

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if value in ('today', 'now'):
            return datetime.date.today()
        elif dateutil_parser:
            return dateutil_parser.parse(value).date()
        else:
            time_struct = time.strptime(value, self.defaultFormat())
            return datetime.date(time_struct.tm_year,
                                 time_struct.tm_month,
                                 time_struct.tm_day)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.defaultFormat())


class DatetimeColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'TIMESTAMP WITHOUT TIME ZONE',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    }

    def __init__(self, **kwds):
        kwds.setdefault('defaultFormat', '%Y-%m-%d %H:%M:%S')

        super(DatetimeColumn, self).__init__(**kwds)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if value in ('today', 'now'):
            return datetime.date.now()
        elif dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            time_struct = time.strptime(value, self.defaultFormat())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.defaultFormat())


class DatetimeWithTimezoneColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'TIMESTAMP WITHOUT TIME ZONE',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    }

    def __init__(self, **kwds):
        kwds.setdefault('defaultFormat', '%Y-%m-%d %H:%M:%S')

        super(DatetimeWithTimezoneColumn, self).__init__(**kwds)


    def dbStore(self, typ, py_value):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>

        :return: <variant>
        """
        if isinstance(py_value, datetime.datetime):
            # ensure we have some timezone information before converting to UTC time
            if py_value.tzinfo is None:
                # match the server information
                tz = pytz.timezone(orb.system.settings().server_timezone)
                py_value = tz.localize(py_value)
            return py_value.astimezone(pytz.utc).replace(tzinfo=None)
        else:
            return super(DatetimeWithTimezoneColumn, self).dbStore(typ, py_value)

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        value = super(DatetimeWithTimezoneColumn, self).restore(value, context)

        if value in ('today', 'now'):
            value = datetime.date.now()

        if isinstance(value, datetime.datetime):
            tz = pytz.timezone(context.timezone)

            if tz is not None:
                if value.tzinfo is None:
                    base_tz = pytz.timezone(orb.system.settings().server_timezone)

                    # the machine timezone and preferred timezone match, so create off utc time
                    if base_tz == tz:
                        value = tz.fromutc(value)

                    # convert the server timezone to a preferred timezone
                    else:
                        value = base_tz.fromutc(value).astimezone(tz)
                else:
                    value = value.astimezone(tz)
            else:
                log.warning('No local timezone defined')

        return value

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, datetime.datetime):
            # ensure we have some timezone information before converting to UTC time
            if value.tzinfo is None:
                # match the server information
                tz = pytz.timezone(orb.system.settings().server_timezone)
                value = tz.localize(value)
            value = value.astimezone(pytz.utc).replace(tzinfo=None)
        return super(DatetimeWithTimezoneColumn, self).store(value, context=context)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            time_struct = time.strptime(value, self.defaultFormat())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.defaultFormat())


class IntervalColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'INTERVAL',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    }

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        return datetime.timedelta(seconds=float(value))

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return str(value.total_seconds())


class TimeColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'TIME',
        'SQLite': 'TEXT',
        'MySQL': 'TIME'
    }

    def __init__(self, **kwds):
        kwds.setdefault('defaultFormat', '%H:%M:%S')

        super(TimeColumn, self).__init__(**kwds)

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if isinstance(db_value, datetime.timedelta):
            hours, remain = divmod(db_value.seconds, 3600)
            minutes, seconds = divmod(remain, 60)
            return datetime.time(hours, minutes, seconds)
        else:
            return super(TimeColumn, self).dbRestore(db_value, context=context)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if value == 'now':
            return datetime.datetime.now().time()
        elif dateutil_parser:
            return dateutil_parser.parse(value).time()
        else:
            time_struct = time.strptime(value, self.defaultFormat())
            return datetime.time(time_struct.tm_hour,
                                 time_struct.tm_min,
                                 time_struct.tm_sec)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.defaultFormat())


class TimestampColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'BIGINT',
        'SQLite': 'INTEGER',
        'MySQL': 'BIGINT'
    }

    def dbRestore(self, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(TimestampColumn, self).dbRestore(db_value, context=context)

    def dbStore(self, typ, py_value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(TimestampColumn, self).dbStore(typ, py_value)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        try:
            return datetime.datetime.fromtimestamp(float(value))
        except StandardError:
            if dateutil_parser:
                return dateutil_parser.parse(value)
            else:
                return datetime.datetime.min()


class UTC_DatetimeColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'TIMESTAMP',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    }

    def __init__(self, **kwds):
        kwds.setdefault('defaultFormat', '%Y-%m-%d %H:%M:%S')

        super(UTC_DatetimeColumn, self).__init__(**kwds)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if value in ('today', 'now'):
            return datetime.datetime.utcnow()
        elif dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            time_struct = time.strptime(value, self.defaultFormat())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.defaultFormat())


class UTC_TimestampColumn(AbstractDatetimeColumn):
    TypeMap = {
        'Postgres': 'BIGINT',
        'SQLite': 'TEXT',
        'MySQL': 'BIGINT'
    }

    def dbRestore(self, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(UTC_TimestampColumn, self).dbRestore(db_value, context=context)

    def dbStore(self, typ, py_value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(UTC_TimestampColumn, self).dbStore(typ, py_value)

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if value in ('today', 'now'):
            return datetime.date.utcnow()

        try:
            return datetime.datetime.fromtimestamp(float(value))
        except StandardError:
            if dateutil_parser:
                return dateutil_parser.parse(value)
            else:
                return datetime.datetime.min()

# register class types
Column.registerAddon('Date', DateColumn)
Column.registerAddon('Datetime', DatetimeColumn)
Column.registerAddon('DatetimeWithTimezone', DatetimeWithTimezoneColumn)
Column.registerAddon('Interval', IntervalColumn)
Column.registerAddon('Time', TimeColumn)
Column.registerAddon('Timestamp', TimestampColumn)
Column.registerAddon('UTC Datetime', UTC_DatetimeColumn)
Column.registerAddon('UTC Timestamp', UTC_TimestampColumn)