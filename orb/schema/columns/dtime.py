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
    pass


class DateColumn(AbstractDatetimeColumn):
    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if value in ('today', 'now'):
            return datetime.date.today()
        elif dateutil_parser:
            return dateutil_parser.parse(value).date()
        else:
            extra = extra or '%Y-%m-%d'
            time_struct = time.strptime(value, extra)
            return datetime.date(time_struct.tm_year,
                                 time_struct.tm_month,
                                 time_struct.tm_day)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        if extra is None:
            extra = '%Y-%m-%d'
        return value.strftime(extra)


class DatetimeColumn(AbstractDatetimeColumn):
    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if out in ('today', 'now'):
            return datetime.date.now()
        elif dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            extra = extra or '%Y-%m-%d %h:%m:s'
            time_struct = time.strptime(value, extra)
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        if extra is None:
            extra = '%Y-%m-%d %h:%m:%s'
        return value.strftime(extra)


class DatetimeWithTimezoneColumn(AbstractDatetimeColumn):
    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        if value in ('today', 'now'):
            return datetime.date.now()
        elif isinstance(value, datetime.datetime):
            tz = self.timezone(context)

            if tz is not None:
                if value.tzinfo is None:
                    base_tz = orb.system.baseTimezone()

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
        else:
            return super(DatetimeWithTimezoneColumn, self).restore(value, context)

    def setTimezone(self, timezone):
        """
        Sets the timezone associated directly to this column.

        :sa     <orb.Manager.setTimezone>

        :param     timezone | <pytz.tzfile> || None
        """
        self._timezone = timezone

    def store(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, datetime.datetime):
            # match the server information
            tz = orb.system.baseTimezone() or self.timezone()
            if tz is not None:
                # ensure we have some timezone information before converting to UTC time
                if value.tzinfo is None:
                    value = tz.localize(value, is_dst=None)

                value = value.astimezone(pytz.utc).replace(tzinfo=None)
            else:
                log.warning('No local timezone defined.')

            return value
        else:
            return super(DatetimeWithTimezoneColumn, self).store(value)

    def timezone(self, context=None):
        """
        Returns the timezone associated specifically with this column.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the system in general.

        :sa     <orb.Manager>

        :param      context | <orb.ContextOptions> || None

        :return     <pytz.tzfile> || None
        """
        return self._timezone or self.schema().timezone(context)

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            extra = extra or '%Y-%m-%d %h:%m:s'
            time_struct = time.strptime(value, extra)
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        if extra is None:
            extra = '%Y-%m-%d %h:%m:%s'
        return value.strftime(extra)

class IntervalColumn(Column):
    pass


class TimeColumn(AbstractDatetimeColumn):
    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if value == 'now':
            return datetime.datetime.now().time()
        elif dateutil_parser:
            return dateutil_parser.parse(value).time()
        else:
            extra = extra or '%h:%m:%s'
            time_struct = time.strptime(value, extra)
            return datetime.time(time_struct.tm_hour,
                                 time_struct.tm_min,
                                 time_struct.tm_sec)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        if extra is None:
            extra = '%h:%m:%s'
        return value.strftime(extra)


class TimestampColumn(AbstractDatetimeColumn):
    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        if isinstance(value, (int, long, float)):
            return datetime.datetime.fromtimestamp(value)
        else:
            return super(TimestampColumn, self).restore(value, context)

    def store(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, datetime.datetime):
            return time.mktime(value.timetuple())
        else:
            return super(TimestampColumn, self).store(value)

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        try:
            return datetime.datetime.fromtimestamp(float(value))
        except StandardError:
            if dateutil_parser:
                return dateutil_parser.parse(value)
            else:
                return datetime.datetime.min()


class UTC_DatetimeColumn(AbstractDatetimeColumn):
    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
        """
        if value in ('today', 'now'):
            return datetime.date.utcnow()
        elif dateutil_parser:
            return dateutil_parser.parse(value)
        else:
            extra = extra or '%Y-%m-%d %h:%m:s'
            time_struct = time.strptime(value, extra)
            return datetime.datetime(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day,
                                     time_struct.tm_hour,
                                     time_struct.tm_minute,
                                     time_struct.tm_sec)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        if extra is None:
            extra = '%Y-%m-%d %h:%m:%s'
        return value.strftime(extra)


class UTC_TimestampColumn(AbstractDatetimeColumn):
    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        if isinstance(value, (int, long, float)):
            return datetime.datetime.fromtimestamp(value)
        else:
            return super(UTC_TimestampColumn, self).restore(value, context)

    def store(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, datetime.datetime):
            return time.mktime(value.timetuple())
        else:
            return super(UTC_TimestampColumn, self).store(value)

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
                    extra | <variant>
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