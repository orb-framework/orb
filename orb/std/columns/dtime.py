import datetime
import demandimport
import logging
import time

from orb.core.column import Column
from orb.core.column_engine import ColumnEngine

with demandimport.enabled():
    import orb
    import pytz

# optional imports
try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None


log = logging.getLogger(__name__)


class DatetimeColumnEngine(ColumnEngine):
    def get_api_value(self, column, plugin_name, db_value, context=None):
        """
        Re-implements the get_api_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param db_value: <variant>
        :param context: <orb.Context>

        :return: <variant> python value
        """
        if db_value is None:
            return None
        try:
            return column.get_api_value(plugin_name, db_value, context=context)
        except NotImplementedError:
            if isinstance(db_value, (str, unicode)):
                return column.value_from_string(db_value, context=context)
            else:
                return super(DatetimeColumnEngine, self).get_api_value(column, plugin_name, db_value, context=context)

    def get_database_value(self, column, plugin_name, py_value, context=None):
        """
        Re-impleents the get_database_value method from ColumnEngine.

        :param column: <orb.Column>
        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant> database value
        """
        if py_value is None:
            return None
        else:
            try:
                return column.get_database_value(plugin_name, py_value, context=context)
            except NotImplementedError:
                return column.value_to_string(py_value, context=context)


class AbstractDatetimeColumn(Column):
    def __init__(self, default_format='%Y-%m-%d %H:%M:%S', **kwds):
        super(AbstractDatetimeColumn, self).__init__(**kwds)

        self.__default_format = default_format

    def default_format(self):
        """
        Returns the default format for this datetime column.

        :return: <str>
        """
        return self.__default_format

    def get_api_value(self, plugin_name, db_value, context=None):
        """
        Abstract method used by the DatetimeColumnEngine to calculate the api value
        per datetime object.

        :param plugin_name: <str>
        :param db_value: <variant>

        :return: <variant>
        """
        raise NotImplementedError

    def get_database_value(self, plugin_name, py_value, context=None):
        """
        Abstract method used by the DatetimeColumnEngine to calculate the database value
        per datetime object.

        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant>
        """
        raise NotImplementedError

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        utc_now = datetime.datetime.utcnow()
        return self.value_from_string(utc_now.strftime(self.default_format()))

    def set_default_format(self, form):
        """
        Sets the default string format for rendering this instance.

        :param form: <str>
        """
        self.__default_format = form


class DateColumn(AbstractDatetimeColumn):
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'DATE',
        'SQLite': 'TEXT',
        'MySQL': 'DATE'
    })

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'TIMESTAMP WITHOUT TIME ZONE',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    })

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'TIMESTAMP WITHOUT TIME ZONE',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    })

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'INTERVAL',
        'SQLite': 'TEXT',
        'MySQL': 'TEXT'
    })

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'TIME',
        'SQLite': 'TEXT',
        'MySQL': 'TIME'
    })

    def __init__(self, **kwds):
        kwds.setdefault('default_format', '%H:%M:%S')

        super(TimeColumn, self).__init__(**kwds)

    def get_api_value(self, plugin_name, db_value, context=None):
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
            return super(TimeColumn, self).get_api_value(plugin_name, db_value, context=context)

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'BIGINT',
        'SQLite': 'INTEGER',
        'MySQL': 'BIGINT'
    })

    def get_api_value(self, plugin_name, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param db_value: <variant>
                    context | <orb.Context> || None
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(TimestampColumn, self).get_api_value(plugin_name, db_value, context=context)

    def get_database_value(self, plugin_name, py_value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param plugin_name: <str>
        :param py_value: <variant>

        :return: <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(TimestampColumn, self).get_database_value(plugin_name, py_value, context=context)

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'TIMESTAMP',
        'SQLite': 'TEXT',
        'MySQL': 'DATETIME'
    })

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
    __default_engine__ = DatetimeColumnEngine(type_map={
        'Postgres': 'BIGINT',
        'SQLite': 'TEXT',
        'MySQL': 'BIGINT'
    })

    def get_api_value(self, plugin_name, db_value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        if isinstance(db_value, (int, long, float)):
            return datetime.datetime.fromtimestamp(db_value)
        else:
            return super(UTC_TimestampColumn, self).get_api_value(plugin_name, db_value, context=context)

    def get_database_value(self, plugin_name, py_value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(py_value, datetime.datetime):
            return time.mktime(py_value.timetuple())
        else:
            return super(UTC_TimestampColumn, self).get_database_value(plugin_name, py_value, context=context)

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

