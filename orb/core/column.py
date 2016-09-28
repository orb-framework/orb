""" Defines the meta information for a column within a table schema. """

import logging
import projex.text
import inflection

from projex.addon import AddonManager
from projex.enum import enum
from projex.lazymodule import lazy_import

log = logging.getLogger(__name__)
orb = lazy_import('orb')


class Column(AddonManager):
    """ Used to define database schema columns when defining Table classes. """
    TypeMap = {}
    MathMap = {
        'Default': {
            'Add': u'{field} + {value}',
            'Subtract': u'{field} - {value}',
            'Multiply': u'{field} * {value}',
            'Divide': u'{field} / {value}',
            'And': u'{field} & {value}',
            'Or': u'{field} | {value}'
        }
    }

    Flags = enum(
        'ReadOnly',
        'Polymorphic',
        'AutoAssign',
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

    def __json__(self):
        default_value = self.default()
        if isinstance(default_value, orb.Model):
            default_value = default_value.id()

        output = {
            'type': self.addonName(),
            'name': self.name(),
            'field': self.field(),
            'display': self.display(),
            'flags': {k: True for k in self.Flags.toSet(self.__flags)},
            'default': default_value
        }
        return output

    def __init__(self,
                 name=None,
                 field=None,
                 display=None,
                 flags=0,
                 default=None,
                 defaultOrder='asc',
                 shortcut='',
                 readPermit=None,
                 writePermit=None,
                 permit=None,
                 getter=None,
                 setter=None,
                 queryFilter=None,
                 schema=None,
                 order=99999):
        # constructor items
        self.__name = name
        self.__field = field
        self.__display = display
        self.__flags = self.Flags.fromSet(flags) if isinstance(flags, set) else flags
        self.__default = default
        self.__defaultOrder = defaultOrder
        self.__shortcut = shortcut
        self.__query_filter = queryFilter
        self.__settermethod = setter
        self.__gettermethod = getter
        self.__readPermit = readPermit or permit
        self.__writePermit = writePermit or permit
        self.__order = order

        # custom options
        self.__schema = None
        self.__timezone = None

        if shortcut:
            self.setFlag(self.Flags.Virtual)

        # auto-register to the schema if provided
        if schema:
            schema.register(self)

    def copy(self):
        """
        Returns a new instance copy of this column.

        :return: <orb.Column>
        """
        out = type(self)(
            name=self.__name,
            field=self.__field,
            display=self.__display,
            flags=self.__flags,
            default=self.__default,
            defaultOrder=self.__defaultOrder,
            getter=self.__gettermethod,
            setter=self.__settermethod,
            queryFilter=self.__query_filter,
            shortcut=self.__shortcut,
            readPermit=self.__readPermit,
            writePermit=self.__writePermit,
            order=self.__order
        )

        return out

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param typ: <str>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # restore translatable column
        if self.testFlag(self.Flags.I18n):
            if isinstance(db_value, (str, unicode)):
                if db_value.startswith('{'):
                    try:
                        value = projex.text.safe_eval(db_value)
                    except StandardError:
                        value = {context.locale: db_value}
                else:
                    value = {context.locale: db_value}
            else:
                value = db_value

            return value
        else:
            return db_value

    def dbMath(self, typ, field, op, value):
        """
        Performs some database math on the given field.  This will be database specific
        implementations and should return the resulting database operation.

        :param field: <str>
        :param op: <orb.Query.Math>
        :param target: <variant>
        :param context: <orb.Context> || None

        :return: <str>
        """
        ops = orb.Query.Math(op)
        format = self.MathMap.get(typ, {}).get(ops) or self.MathMap.get('Default').get(ops) or '{field}'
        return format.format(field=field, value=value)

    def dbStore(self, typ, py_value):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # convert base types to work in the database
        if isinstance(py_value, (list, tuple, set)):
            py_value = tuple((self.dbStore(x) for x in py_value))
        elif isinstance(py_value, orb.Collection):
            py_value = py_value.ids()
        elif isinstance(py_value, orb.Model):
            py_value = py_value.id()

        return py_value

    def dbType(self, typ):
        """
        Returns the database object type based on the given connection type.

        :param typ:  <str>

        :return: <str>
        """
        return self.TypeMap.get(typ, self.TypeMap.get('Default'))

    def default(self):
        """
        Returns the default value for this column to return
        when generating new instances.

        :return     <variant>
        """
        if isinstance(self.__default, (str, unicode)):
            return self.valueFromString(self.__default)
        else:
            return self.__default

    def defaultOrder(self):
        """
        Returns the default ordering for this column when sorting.

        :return     <str>
        """
        return self.__defaultOrder

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
        if not self.__field:
            default_field = inflection.underscore(self.__name)
            if isinstance(self, orb.ReferenceColumn):
                default_field += '_id'
            self.__field = default_field

        return self.__field or default_field

    def firstMemberSchema(self, schemas):
        """
        Returns the first schema within the list that this column is a member
        of.

        :param      schemas | [<orb.TableSchema>, ..]

        :return     <orb.TableSchema> || None
        """
        for schema in schemas:
            if schema.hasColumn(self):
                return schema
        return self.schema()

    def flags(self):
        """
        Returns the flags that have been set for this column.

        :return     <Column.Flags>
        """
        return self.__flags

    def getter(self):
        def wrapped(func):
            self.__gettermethod = func
            return func
        return wrapped

    def gettermethod(self):
        return self.__gettermethod

    def isMemberOf(self, schemas):
        """
        Returns whether or not this column is a member of any of the given
        schemas.

        :param      schemas | [<orb.TableSchema>, ..] || <orb.TableSchema>

        :return     <bool>
        """
        if type(schemas) not in (tuple, list, set):
            schemas = (schemas,)

        for schema in schemas:
            if schema.hasColumn(self):
                return True
        return False

    def isNull(self, value):
        """
        Returns whether or not the given value is considered null for this column.

        :param value: <variant>

        :return: <bool>
        """
        return value is None

    def loadJSON(self, jdata):
        """
        Initializes the information for this class from the given JSON data blob.

        :param jdata: <dict>
        """
        # required params
        self.__name = jdata['name']
        self.__field = jdata['field']

        # optional fields
        self.__display = jdata.get('display') or self.__display
        self.__flags = jdata.get('flags') or self.__flags
        self.__defaultOrder = jdata.get('defaultOrder') or self.__defaultOrder
        self.__default = jdata.get('default') or self.__default

    def memberOf(self, schemas):
        """
        Returns a list of schemas this column is a member of from the inputted
        list.

        :param      schemas | [<orb.TableSchema>, ..]

        :return     [<orb.TableSchema>, ..]
        """
        for schema in schemas:
            if schema.hasColumn(self):
                yield schema

    def name(self):
        """
        Returns the accessor name that will be used when
        referencing this column around the app.

        :return     <str>
        """
        return self.__name

    def order(self):
        """
        Returns the priority order for this column.  Lower
        number priorities will be processed first.

        :return: <int>
        """
        return self.__order

    def queryFilter(self, function=None):
        """
        Defines a decorator that can be used to filter
        queries.  It will assume the function being associated
        with the decorator will take a query as an input and
        return a modified query to use.

        :usage

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.queryFilter()
                def objectsFilter(cls, query, **context):
                    return orb.Query()

        :param function: <callable>

        :return: <wrapper>
        """
        if function is not None:
            self.__query_filter = function
            return function

        def wrapper(func):
            self.__query_filter = func
            return func

        return wrapper

    def queryFilterMethod(self):
        """
        Returns the actual query filter method, if any,
        that is associated with this collector.

        :return: <callable>
        """
        return self.__query_filter

    def random(self):
        """
        Returns a random value that fits this column's parameters.

        :return: <variant>
        """
        return self.default()

    def readPermit(self):
        """
        Returns the read permit for this column.

        :return: <str> || None
        """
        return self.__readPermit

    def restore(self, value, context=None, inflated=True):
        """
        Restores the value from a table cache for usage.

        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        context = context or orb.Context()

        # check to see if this column is translatable before restoring
        if self.testFlag(self.Flags.I18n):
            locales = context.locale.split(',')

            if not isinstance(value, dict):
                default_locale = locales[0]
                if default_locale == 'all':
                    default_locale = orb.system.settings().default_locale
                value = {default_locale: value}

            if 'all' in locales:
                return value

            if len(locales) == 1:
                return value.get(locales[0])
            else:
                return {locale: value.get(locale) for locale in locales}
        else:
            return value

    def setShortcut(self, shortcut):
        """
        Sets the shortcut information for this column.

        :param shortcut: <str>
        """
        self.__shortcut = shortcut

    def shortcut(self):
        """
        Returns the shortcut path that will be taken through
        this column.

        :return: <str> || None
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
            value = self.valueFromString(value)

        # store the internationalized property
        if self.testFlag(self.Flags.I18n):
            if not isinstance(value, dict):
                context = context or orb.Context()
                return {context.locale: value}
            else:
                return value
        else:
            return value

    def schema(self):
        """
        Returns the table that this column is linked to in the database.

        :return     <TableSchema>
        """
        return self.__schema

    def settermethod(self):
        return self.__settermethod

    def setter(self):
        def wrapped(func):
            self.__settermethod = func
            return func
        return wrapped

    def setDefault(self, default):
        """
        Sets the default value for this column to the inputted value.

        :param      default | <str>
        """
        self.__default = default

    def setDisplay(self, display):
        """
        Sets the display name for this column.

        :param      displayName | <str>
        """
        self.__display = display

    def setField(self, field):
        """
        Sets the field name for this column.

        :param      field | <str>
        """
        self.__field = field

    def setFlag(self, flag, state=True):
        """
        Sets whether or not this flag should be on.

        :param      flag  | <Column.Flags>
                    state | <bool>
        """
        if state:
            self.__flags |= flag
        else:
            self.__flags &= ~flag

    def setFlags(self, flags):
        """
        Sets the global flags for this column to the inputted flags.

        :param      flags | <Column.Flags>
        """
        self.__flags = flags

    def setName(self, name):
        """
        Sets the name of this column to the inputted name.

        :param      name    | <str>
        """
        self.__name = name

    def setReadPermit(self, permit):
        """
        Sets the read permission for this column.

        :param permit: <str> || None
        """
        self.__readPermit = permit

    def setSchema(self, schema):
        self.__schema = schema

    def setOrder(self, order):
        """
        Sets the priority order for this column.  Lower
        orders will be processed first during updates
        (in case one column should be set before another).

        :param order: <int>
        """
        self.__order = order

    def setWritePermit(self, permit):
        """
        Sets the write permission for this column.

        :param permit: <str> || None
        """
        self.__writePermit = permit

    def testFlag(self, flag):
        """
        Tests to see if this column has the inputted flag set.

        :param      flag | <Column.Flags>
        """
        if isinstance(flag, (str, unicode)):
            flag = self.Flags(flag)

        return bool(self.flags() & flag) if flag >= 0 else not bool(self.flags() & ~flag)

    def validate(self, value):
        """
        Validates the inputted value against this columns rules.  If the inputted value does not pass, then
        a validation error will be raised.  Override this method in column sub-classes for more
        specialized validation.

        :param      value | <variant>

        :return     <bool> success
        """
        # check for the required flag
        if self.testFlag(self.Flags.Required) and not self.testFlag(self.Flags.AutoAssign):
            if self.isNull(value):
                msg = '{0} is a required column.'.format(self.name())
                raise orb.errors.ColumnValidationError(self, msg)

        # otherwise, we're good
        return True

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :param      value | <str>
        """
        return projex.text.nativestring(value)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.

        :sa         engine

        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value)

    def writePermit(self):
        """
        Returns the permit required to modify a particular column.

        :return: <str> || None
        """
        return self.__writePermit

    @classmethod
    def fromJSON(cls, jdata):
        """
        Generates a new column from the given json data.  This should
        be already loaded into a Python dictionary, not a JSON string.

        :param      jdata | <dict>

        :return     <orb.Column> || None
        """
        cls_type = jdata.get('type')
        col_cls = cls.byName(cls_type)

        if not col_cls:
            raise orb.errors.ColumnTypeNotFound(cls_type)
        else:
            col = col_cls()
            col.loadJSON(jdata)
            return col
