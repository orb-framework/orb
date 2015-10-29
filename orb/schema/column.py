""" Defines the meta information for a column within a table schema. """

import datetime
import decimal
import logging
import projex.regex
import projex.text
import time

from projex.addon import AddonManager
from projex.enum import enum
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree

from ..common import ColumnType, RemovedAction

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')
errors = lazy_import('orb.errors')


class Column(AddonManager):
    """ Used to define database schema columns when defining Table classes. """
    class Index(object):
        def __init__(self, name='', cached=False, timeout=None):
            self.name = name
            self.cached = cached
            self.timeout = timeout

    # define default naming system
    TEMPLATE_PRIMARY_KEY = 'id'
    TEMPLATE_GETTER = '[name::camelHump::lower_first]'
    TEMPLATE_SETTER = 'set[name::camelHump::upper_first::lstrip(Is)]'
    TEMPLATE_FIELD = '[name::underscore::lower]'
    TEMPLATE_DISPLAY = '[name::upper_first::words]'
    TEMPLATE_INDEX = 'by[name::camelHump::upper_first]'
    TEMPLATE_REVERSED = '[name::reversed::camelHump::lower_first]'
    TEMPLATE_REFERENCE = '[name::underscore::lower]_id'

    TEMPLATE_MAP = {
        'getterName': TEMPLATE_GETTER,
        'setterName': TEMPLATE_SETTER,
        'fieldName': TEMPLATE_FIELD,
        'displayName': TEMPLATE_DISPLAY,
        'indexName': TEMPLATE_INDEX,
        'primaryKey': TEMPLATE_PRIMARY_KEY,
    }

    Flags = enum(
        'ReadOnly',             # 1
        'Private',              # 2
        'Referenced',           # 4
        'Polymorphic',          # 8
        'Primary',              # 16
        'AutoIncrement',        # 32
        'Required',             # 64
        'Unique',               # 128
        'Encrypted',            # 256
        'Searchable',           # 512
        'IgnoreByDefault',      # 1024
        'Translatable',         # 2048
        'CaseSensitive'         # 4096
    )

    def __init__(self,
                 name=None,
                 field=None,
                 display=None,
                 index=None,
                 flags=None,
                 default=None,
                 defaultOrder='asc'):
        # constructor items
        self._name = name
        self._field = field
        self._display = display
        self._index = index
        self._flags = flags
        self._default = default
        self._defaultOrder = defaultOrder

        # custom options
        self._schema = None
        self._timezone = None
        self._customData = {}

    def customData(self, key, default=None):
        """
        Returns custom information that was assigned to this column for the \
        inputted key.  If no value was assigned to the given key, the inputted \
        default value will be returned.
        
        :param      key     | <str>
                    default | <variant>
            
        :return     <variant>
        """
        return self._customData.get(key, default)

    def default(self, resolve=False):
        """
        Returns the default value for this column to return
        when generating new instances.
        
        :return     <variant>
        """
        if isinstance(self._default, (str, unicode)):
            return self.valueFromString(self._default)
        else:
            return self._default

    def defaultOrder(self):
        """
        Returns the default ordering for this column when sorting.

        :return     <str>
        """
        return self._defaultOrder

    def display(self):
        """
        Returns the display text for this column.

        :return     <str>
        """
        return self._display or projex.text.pretty(self._name)

    def field(self):
        """
        Returns the field name that this column will have inside the database.
                    
        :return     <str>
        """
        return self._field

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
        return self._flags

    def index(self):
        """
        Returns the index information for this column, if any.

        :return:    <orb.Column.Index> || None
        """
        return self._index

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
        return type(value) is not bool and not bool(value)

    def loadJSON(self, jdata):
        """
        Initializes the information for this class from the given JSON data blob.

        :param jdata: <dict>
        """
        # required params
        self._name = jdata['name']
        self._field = jdata['field']

        # optional fields
        self._display = jdata.get('display') or self._display
        self._flags = jdata.get('flags') or self._flags
        self._defaultOrder = jdata.get('defaultOrder') or self._defaultOrder
        self._default = jdata.get('default') or self._default

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
        return self._name

    def restore(self, value, context=None):
        """
        Restores the value from a table cache for usage.
        
        :param      value   | <variant>
                    context | <orb.ContextOptions> || None
        """
        return value

    def store(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary
        
        :param      value | <variant>
        
        :return     <variant>
        """
        if isinstance(value, (str, unicode)):
            value = self.valueFromString(value)

        return value

    def schema(self):
        """
        Returns the table that this column is linked to in the database.
        
        :return     <TableSchema>
        """
        return self._schema

    def setCustomData(self, key, value):
        """
        Sets the custom data at the inputted key to the given value.
        
        :param      key     | <str>
                    value   | <variant>
        """
        self._customData[nstr(key)] = value

    def setDefault(self, default):
        """
        Sets the default value for this column to the inputted value.
        
        :param      default | <str>
        """
        self._default = default

    def setDisplay(self, display):
        """
        Sets the display name for this column.
        
        :param      displayName | <str>
        """
        self._display = display

    def setField(self, field):
        """
        Sets the field name for this column.
        
        :param      field | <str>
        """
        self._field = field

    def setFlag(self, flag, state=True):
        """
        Sets whether or not this flag should be on.
        
        :param      flag  | <Column.Flags>
                    state | <bool>
        """
        if state:
            self._flags |= flag
        else:
            self._flags &= ~flag

    def setFlags(self, flags):
        """
        Sets the global flags for this column to the inputted flags.
        
        :param      flags | <Column.Flags>
        """
        self._flags = flags

    def setName(self, name):
        """
        Sets the name of this column to the inputted name.
        
        :param      name    | <str>
        """
        self._name = name

    def setIndex(self, index):
        """
        Sets the index instance for this column to the inputted instance.
        
        :param      index   | <orb.Column.Index> || None
        """
        self._index = index

    def testFlag(self, flag):
        """
        Tests to see if this column has the inputted flag set.
        
        :param      flag | <Column.Flags>
        """
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
        if self.testFlag(self.Flags.Required) and not self.testFlag(self.Flags.AutoIncrement):
            if self.isNull(value):
                msg = '{0} is a required column.'.format(self.name())
                raise errors.ColumnValidationError(self, msg)

        # otherwise, we're good
        return True

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :sa         engine
        
        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value)

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
            raise orb.errors.InvalidColumnType(cls_type)
        else:
            col = col_cls()
            col.loadJSON(jdata)
            return col