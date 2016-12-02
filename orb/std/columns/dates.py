import datetime
import demandimport
import logging
import time

from orb.core.column import Column

with demandimport.enabled():
    import orb
    import pytz

# optional imports
try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None


log = logging.getLogger(__name__)


class AbstractDatetimeColumn(Column):
    def __init__(self, default_format='%Y-%m-%d %H:%M:%S', **kwds):
        super(AbstractDatetimeColumn, self).__init__(**kwds)

        # define custom properties
        self.__default_format = default_format

    def database_restore(self, db_value, context=None):
        """
        Re-implements the `orb.Column.database_restore` method to convert
        a database-stored value to a python value.

        :param db_value: <variant>
        :param context: <orb.Context> or None

        :return: <datetime.datetime>
        """
        if db_value is None:
            return None
        elif isinstance(db_value, (str, unicode)):
            return self.value_from_string(db_value, context=context)
        else:
            return super(AbstractDatetimeColumn, self).database_restore(db_value, context=context)

    def database_store(self, py_value, context=None):
        """
        Re-implements the `orb.Column.database_store` method to convert
        a python value to something that can be stored in a database.

        :param py_value: <variant>
        :param context: <orb.Context> or None

        :return: <variant>
        """
        if py_value is None:
            return None
        else:
            return self.value_to_string(py_value, context=context)

    def default_format(self):
        """
        Returns the default format for this datetime column.

        :return: <str>
        """
        return self.__default_format

    def set_default_format(self, form):
        """
        Sets the default string format for rendering this instance.

        :param form: <str>
        """
        self.__default_format = form


class DateColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d')

        super(DateColumn, self).__init__(**kwds)

    def value_from_string(self, value, context=None):
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
            time_struct = time.strptime(value, self.default_format())
            return datetime.date(time_struct.tm_year,
                                 time_struct.tm_month,
                                 time_struct.tm_day)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class DatetimeColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d %H:%M:%S')

        super(DatetimeColumn, self).__init__(**kwds)

    def value_from_string(self, value, context=None):
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
            time_struct = time.strptime(value, self.default_format())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class DatetimeWithTimezoneColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d %H:%M:%S')

        super(DatetimeWithTimezoneColumn, self).__init__(**kwds)

    def get_database_value(self, plugin_name, py_value, context=None):
        """
        Prepares to store this column for the a particular backend database.

        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant>
        """
        if isinstance(py_value, datetime.datetime):
            # ensure we have some timezone information before converting to UTC time
            if py_value.tzinfo is None:
                # match the server information
                tz = pytz.timezone(orb.system.settings.server_timezone)
                py_value = tz.localize(py_value)
            return py_value.astimezone(pytz.utc).replace(tzinfo=None)
        else:
            return super(DatetimeWithTimezoneColumn, self).get_database_value(plugin_name, py_value, context=context)

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
                    base_tz = pytz.timezone(orb.system.settings.server_timezone)

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
                tz = pytz.timezone(orb.system.settings.server_timezone)
                value = tz.localize(value)
            value = value.astimezone(pytz.utc).replace(tzinfo=None)
        return super(DatetimeWithTimezoneColumn, self).store(value, context=context)

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        if dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            time_struct = time.strptime(value, self.default_format())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class IntervalColumn(AbstractDatetimeColumn):
    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        return datetime.timedelta(seconds=float(value))

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return str(value.total_seconds())


class TimeColumn(AbstractDatetimeColumn):
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%H:%M:%S')

        super(TimeColumn, self).__init__(**kwds)

    def database_restore(self, db_value, context=None):
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
            return super(TimeColumn, self).database_restore(db_value, context=context)

    def value_from_string(self, value, context=None):
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
            time_struct = time.strptime(value, self.default_format())
            return datetime.time(time_struct.tm_hour,
                                 time_struct.tm_min,
                                 time_struct.tm_sec)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class TimestampColumn(AbstractDatetimeColumn):
    def database_restore(self, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param db_value: <variant>
        :param context: <orb.Context> or None

        :return: <datetime>
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(TimestampColumn, self).database_restore(db_value, context=context)

    def database_store(self, py_value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param py_value: <variant>
        :param context: <orb.Context> or None

        :return: <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(TimestampColumn, self).database_store(py_value, context=context)

    def value_from_string(self, value, context=None):
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
    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%Y-%m-%d %H:%M:%S')

        super(UTC_DatetimeColumn, self).__init__(**kwds)

    def value_from_string(self, value, context=None):
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
            time_struct = time.strptime(value, self.default_format())
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
        """
        return value.strftime(self.default_format())


class UTC_TimestampColumn(AbstractDatetimeColumn):
    def database_restore(self, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param db_value: <variant>
        :param context: <orb.Context> or None

        :return: <variant>
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(UTC_TimestampColumn, self).database_restore(db_value, context=context)

    def database_store(self, py_value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(UTC_TimestampColumn, self).database_store(py_value, context=context)

    def value_from_string(self, value, context=None):
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

