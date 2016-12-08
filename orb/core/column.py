""" Defines the meta information for a column within a table schema. """

import demandimport
import logging
import inflection
import json

from ..utils.enum import enum
from ..utils.text import nativestring

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class Column(object):
    """ Used to define database schema columns when defining Table classes. """
    Flags = enum(
        'ReadOnly',
        'Polymorphic',
        'AutoAssign',
        'AutoIncrement',
        'Required',
        'Unique',
        'Encrypted',
        'Searchable',
        'I18n',
        'I18n_NoDefault',
        'CaseSensitive',
        'Virtual',
        'Static',
        'Protected',
        'Private',
        'AutoExpand',
        'RequiresExpand',
        'Keyable'
    )

    def __init__(self,

                 # class properties
                 name='',
                 shortcut='',
                 display='',
                 flags=0,
                 default=None,
                 schema=None,
                 order=99999,
                 keywords=None,
                 properties=None,
                 validators=None,
                 value_filters=None,

                 # database properties
                 alias='',
                 field='',

                 # permissions
                 permit=None,
                 read_permit=None,
                 write_permit=None,

                 # methods
                 getter=None,
                 setter=None,
                 filter=None):
        # class properties
        self.__name = name
        self.__display = display
        self.__shortcut = shortcut
        self.__flags = self.Flags.from_set(flags) if isinstance(flags, set) else flags
        self.__default = default
        self.__order = order
        self.__keywords = keywords.copy() if keywords else {}
        self.__properties = properties or {}
        self.__validators = validators or []
        self.__value_filters = value_filters or []

        # database properties
        self.__alias = alias
        self.__field = field

        # permissions
        self.__read_permit = read_permit or permit
        self.__write_permit = write_permit or permit

        # methods
        self.__filtermethod = filter
        self.__settermethod = setter
        self.__gettermethod = getter

        # custom options
        self.__schema = None
        self.__timezone = None

        # shortcut columns are virtual by definition
        if shortcut:
            self.set_flag(self.Flags.Virtual)

        # auto-register to the schema if provided
        if schema:
            schema.register(self)

    def __cmp__(self, other):
        if self is other:
            return 0
        elif isinstance(other, Column):
            return cmp(self.order(), other.order())
        else:
            return -1

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __iter__(self):
        for key in ('name',
                    'display',
                    'shortcut',
                    'flags',
                    'default',
                    'order',
                    'keywords',
                    'alias',
                    'field',
                    'gettermethod',
                    'settermethod',
                    'filtermethod',
                    'read_permit',
                    'write_permit',
                    'properties',
                    'validators',
                    'value_filters'):
            value = getattr(self, '_Column__{0}'.format(key))
            if key.endswith('method'):
                yield key.replace('method', ''), value
            else:
                yield key, value

    def __json__(self):
        """
        Serializes this column to give to the frontend.

        :return: <dict>
        """
        default_value = self.default()
        if isinstance(default_value, orb.Model):
            default_value = default_value.id()

        output = {
            'type': self.get_plugin_name(),
            'name': self.name(),
            'field': self.alias(),
            'display': self.display(),
            'flags': {k: True for k in self.Flags.to_set(self.__flags)},
            'default': default_value,
            'properties': self.__properties
        }
        return output

    def add_keyword(self, keyword, value):
        """
        Registers a keyword for this column to be able to process
        custom value information.

        Args:
            keyword: <str>
            value: <variant>

        Returns:
            None

        """
        self.__keywords[keyword] = value

    def add_validator(self, validator):
        """
        Adds a validator to the list for this column.

        Args:
            validator: <callable>

        """
        self.__validators.append(validator)

    def add_value_filter(self, filter):
        """
        Adds a filter that will be used to process a value that is to be stored
        information for this column on a model.  The filter should accept
        a single argument, the value to be filtered, and return the filtered result.

        Args:
            filter: <callable>

        Returns:
            <variant>

        """
        self.__value_filters.append(filter)

    def alias(self):
        """
        Returns the alias to use when extracting data from the backend
        as raw data.

        :return: <str>
        """
        return self.__alias or self.default_alias()

    def copy(self, **kw):
        """
        Returns a new instance copy of this column.

        :return: <orb.Column>
        """
        data = dict(self)
        data.update(kw)
        return type(self)(**data)

    def default(self):
        """
        Returns the default value for this column to return
        when generating new instances.

        :return: <variant>
        """
        if isinstance(self.__default, basestring):
            return self.value_from_string(self.__default)
        elif callable(self.__default):
            return self.__default(self)
        else:
            return self.__default

    def default_alias(self):
        """
        Returns the default alias name for this column.  By default,
        this will be an underscored version of the column name.

        Returns:
            <str>

        """
        return inflection.underscore(self.__name)

    def display(self):
        """
        Returns the display text for this column.

        :return     <str>
        """
        return self.__display or inflection.titleize(self.__name)

    def field(self):
        """
        Returns the field name that this column will have inside the database.

        :return     <str>
        """
        return self.__field or self.alias()

    def flags(self):
        """
        Returns the flags that have been set for this column.

        :return     <Column.Flags>
        """
        return self.__flags

    def filter(self, function=None):
        """
        Defines a decorator that can be used to filter
        queries.  It will assume the function being associated
        with the decorator will take a query as an input and
        return a modified query to use.

        :param function: <callable> or None

        :usage

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.filter()
                def objectsFilter(cls, query, **context):
                    return orb.Query()

        :param function: <callable>

        :return: <wrapper>
        """
        if function is not None:
            self.__filtermethod = function
            return function
        else:
            def wrapper(f):
                self.__filtermethod = f
                return f

            return wrapper

    def filter_value(self, value):
        """
        Runs the filters for this column on the given value.

        Args:
            value: <variant>

        Returns:
            <variant>

        """
        for filter in self.__value_filters:
            value = filter(value)
        return value

    def filtermethod(self):
        """
        Returns the actual query filter method, if any,
        that is associated with this collector.

        :return: <callable> or None
        """
        return self.__filtermethod

    def getter(self, function=None):
        """
        Decorator used for assigning the gettermethod of this
        column to a function.

        :param funciton: <callable> or None

        :usage:

            column = Column()

            @column.getter
            def get_column_value():
                return 10

            @column.getter()
            def get_column_value():
                return 10

        :return: <callable>
        """
        if function is not None:
            self.__gettermethod = function
            return function
        else:
            def wrapper(f):
                self.__gettermethod = f
                return f
            return wrapper

    def gettermethod(self):
        """
        Returns a custom getter method override for
        accessing this columns values.

        :return: <callable> or None
        """
        return self.__gettermethod

    def is_empty(self, value):
        """
        Returns whether or not the given value is considered "empty" by
        the column.

        :param value: <variant>

        :return: <bool>
        """
        return not bool(value)

    def is_null(self, value):
        """
        Returns whether or not the given value is considered "null" by the
        column.

        :param value: <variant>

        :return: <bool>
        """
        return value is None

    def iter_flags(self):
        """
        Iterates over the flag set for this column, yielding
        each individual flag value.

        :return: <generator>
        """
        return orb.Column.Flags.iter_values(self.flags())

    def keywords(self):
        """
        Returns a list of the acceptable keywords that
        can be used for values associated with this column.

        Returns:
            [<str>, ..]

        """
        return self.__keywords.keys()

    def keyword_value(self, keyword, context=None):
        """
        Returns the value associated the given keyword
        for this column.  This method will raise a KeyError
        if no matching keyword is found.

        Args:
            keyword: <str>
            context: <orb.Context>

        Returns:
            <variant>
        """
        value = self.__keywords[keyword]
        if callable(value):
            return value(keyword, context)
        else:
            return value

    def name(self):
        """
        Returns the accessor name that will be used when
        referencing this column around the app.

        :return: <str>
        """
        return self.__name

    def order(self):
        """
        Returns the priority order for this column.  Lower
        number priorities will be processed first.

        :return: <int>
        """
        return self.__order

    def properties(self):
        """
        Returns a dictionary that can be used to store and propagate custom
        properties about this column through the system.  The column class itself
        does not do anything with these properties, this is just a system to be
        able to transfer meta data about the column around.  This dictionary
        will serialize for any frontend services as well.

        Returns:
            <dict>

        """
        return self.__properties

    def random_value(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return self.default()

    def read_permit(self):
        """
        Returns the read permit for this column.

        :return: <str> or None
        """
        return self.__read_permit

    def remove_value_filter(self, filter):
        """
        Removes the given validator from the stack for this column.

        Args:
            filter: <callable>

        """
        try:
            self.__value_filters.remove(filter)
        except ValueError:  # pragma: no cover
            pass

    def remove_validator(self, validator):
        """
        Removes the given validator from the stack for this column.

        Args:
            validator: <callable>

        """
        try:
            self.__validators.remove(validator)
        except ValueError:  # pragma: no cover
            pass

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.

        :param value: <variant>
        :param context: <orb.Context> or None
        """
        context = context or orb.Context()

        # check to see if this column is translatable before restoring
        if self.test_flag(self.Flags.I18n):
            # restore json data
            if isinstance(value, (str, unicode)) and value.startswith('{') and value.endswith('}'):
                try:
                    value = json.loads(value)
                except Exception:
                    pass

            locales = context.locale.split(',')

            if not isinstance(value, dict):
                default_locale = locales[0]
                if default_locale == 'all':
                    schema = self.schema()
                    system = schema.system() if schema else orb.system
                    default_locale = system.settings.default_locale
                value = {default_locale: value}

            if 'all' in locales:
                return value

            if len(locales) == 1:
                return value.get(locales[0])
            else:
                return {locale: value.get(locale) for locale in locales}
        else:
            return value

    def schema(self):
        """
        Returns the table that this column is linked to in the database.

        :return     <TableSchema>
        """
        return self.__schema

    def set_alias(self, alias):
        """
        Sets the column alias that is used for this instance.

        :param alias: <str>
        """
        self.__alias = alias

    def set_default(self, default):
        """
        Sets the default value for this column to the inputted value.

        :param default: <variant>
        """
        self.__default = default

    def set_display(self, display):
        """
        Sets the display name for this column.

        :param display: <str>
        """
        self.__display = display

    def set_field(self, field):
        """
        Sets the field name for this column.

        :param field: <str>
        """
        self.__field = field

    def set_flag(self, flag, state=True):
        """
        Sets whether or not this flag should be on.

        :param flag: <orb.Column.Flags>
        :param state: <bool>
        """
        if state:
            self.__flags |= flag
        else:
            self.__flags &= ~flag

    def set_flags(self, flags):
        """
        Sets the global flags for this column to the inputted flags.

        :param flags: <orb.Column.Flags>
        """
        self.__flags = flags

    def set_name(self, name):
        """
        Sets the name of this column to the inputted name.

        :param name: <str>
        """
        self.__name = name

    def set_read_permit(self, permit):
        """
        Sets the read permission for this column.

        :param permit: <str> or None
        """
        self.__read_permit = permit

    def set_schema(self, schema):
        """
        Assigns the schema reference for this column.

        :param schema: <orb.Schema>
        """
        self.__schema = schema

    def set_order(self, order):
        """
        Sets the priority order for this column.  Lower
        orders will be processed first during updates
        (in case one column should be set before another).

        :param order: <int>
        """
        self.__order = order

    def set_shortcut(self, shortcut):
        """
        Sets the shortcut information for this column.  If
        a shortcut is provided, this method will also automatically
        set the `Virtual` flag for the column, since, by definition,
        shortcut columns are virtual.

        :param shortcut: <str>
        """
        self.__shortcut = shortcut
        if shortcut:
            self.set_flag(Column.Flags.Virtual)

    def set_write_permit(self, permit):
        """
        Sets the write permission for this column.

        :param permit: <str> or None
        """
        self.__write_permit = permit

    def setter(self, function=None):
        """
        Decorator used for assigning the settermethod of this
        column to a function.

        :param function: <callable> or None

        :usage:

            column = Column()

            @column.setter
            def set_column_value(value):
                pass

            @column.setter()
            def set_column_value(value):
                pass

        :return: <callable>
        """
        if function is not None:
            self.__settermethod = function
            return function
        else:
            def wrapper(f):
                self.__settermethod = f
                return f
            return wrapper

    def settermethod(self):
        """
        Returns a custom setter method override for
        accessing this columns values.

        :return: <callable> or None
        """
        return self.__settermethod

    def shortcut(self):
        """
        Returns the shortcut path that will be taken through
        this column.

        :return: <str> or None
        """
        return self.__shortcut

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary

        :param      value | <variant>

        :return     <variant>
        """
        if isinstance(value, (str, unicode)):
            value = self.value_from_string(value)

        # store the internationalized property
        if self.test_flag(self.Flags.I18n):
            if not isinstance(value, dict):
                context = context or orb.Context()
                return {context.locale: self.filter_value(value)}
            else:
                return {locale: self.filter_value(v) for locale, v in value.items()}
        else:
            return self.filter_value(value)

    def test_flag(self, flag):
        """
        Tests to see if this column has the inputted flag set.

        :param flag: <orb.Column.Flags> or <int> or <str> or <set>

        :return: <bool>
        """
        return self.Flags.test_flag(self.flags(), flag)

    def validate(self, value):
        """
        Validates the inputted value against this columns rules.  If the inputted value does not pass, then
        a validation error will be raised.  Override this method in column sub-classes for more
        specialized validation.

        :param value: <variant>

        :return: <bool>
        """
        # check for the required flag
        if self.test_flag(self.Flags.Required) and not self.test_flag(self.Flags.AutoAssign | self.Flags.AutoIncrement):
            if self.is_null(value):
                msg = '{0} is a required column.'.format(self.name())
                raise orb.errors.ColumnValidationError(self, msg)

        # go through custom validators, if any
        for validator in self.__validators:
            if not validator(value):
                raise orb.errors.ColumnValidationError(self, 'Failed to validate value')

        # otherwise, we're good
        return True

    def value_filters(self):
        """
        Returns a list of the value filters that will be run on a
        value when it is stored for this column.

        Returns:
            [<callable>, ..]

        """
        return self.__value_filters

    def validators(self):
        """
        Returns a list of the validators associated with this column.

        Returns:
            [<callable>, ..]

        """
        return self.__validators

    def value_from_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param value: <str>
        """
        return nativestring(value)

    def value_to_string(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param value: <str>
        :param context: <orb.Context>
        """
        return nativestring(value)

    def write_permit(self):
        """
        Returns the permit required to modify a particular column.

        :return: <str> or None
        """
        return self.__write_permit

    @classmethod
    def get_base_types(cls):
        """
        Returns all the <orb.Column> classes that this column inherits from.

        :return: <generator>
        """
        curr_level = cls.__bases__
        while curr_level:
            next_level = []
            for base in curr_level:
                if issubclass(base, orb.Column):
                    yield base
                    next_level.extend(base.__bases__)
            curr_level = next_level

    @classmethod
    def get_plugin_name(cls):
        """
        Returns the plugin name for this column.  By default, this will
        be the name of the column class, minus the trailing `Column` text.
        To override a class's plugin name, set the `__plugin_name__` attribute
        on the class.

        :return: <str>
        """
        default = cls.__name__.replace('Column', '') or cls.__name__
        return getattr(cls, '__plugin_name__', default)

