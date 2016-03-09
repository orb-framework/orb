""" Defines the meta information for a column within a table schema. """

import datetime
import decimal
import logging
import projex.regex
import projex.text
import time

from projex.enum import enum
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree

from ..common import ColumnType, RemovedAction

try:
    from dateutil import parser as dateutil_parser
except ImportError:
    dateutil_parser = None

try:
    from webhelpers.html import tools as html_tools
except ImportError:
    html_tools = None

try:
    import bleach
except ImportError:
    bleach = None

log = logging.getLogger(__name__)
orb = lazy_import('orb')
pytz = lazy_import('pytz')
errors = lazy_import('orb.errors')


class Column(object):
    """ Used to define database schema columns when defining Table classes. """

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

    Flags = enum('ReadOnly',
                 'Private',
                 'Referenced',
                 'Polymorphic',
                 'Primary',
                 'AutoIncrement',
                 'Required',
                 'Unique',
                 'Encrypted',
                 'Searchable',
                 'IgnoreByDefault',
                 'Translatable',
                 'CaseSensitive')

    Kind = enum('Field',
                'Joined',
                'Aggregate',
                'Proxy')

    def __str__(self):
        return self.name() or self.fieldName() or '<< INVALID COLUMN >>'

    def __init__(self, *args, **options):
        # support 2 constructor methods orb.Column(typ, name) or
        # orb.Column(name, type=typ)
        if len(args) == 2:
            options['type'], options['name'] = args
        elif len(args) == 1:
            options['name'] = args[0]


        # define required arguments
        self._name = options.get('name', '')
        self._type = options.get('type', None)
        self._schema = options.get('schema')
        self._engines = {}
        self._customData = {}
        self._timezone = None
        self._shortcut = options.get('shortcut', '')

        # set default values
        ref = options.get('reference', '')
        for key in Column.TEMPLATE_MAP.keys():
            if key not in options:
                options[key] = Column.defaultDatabaseName(key, self._name, ref)

        # naming & accessor options
        self._getter = options.get('getter')
        self._getterName = options.get('getterName')
        self._setter = options.get('setter')
        self._setterName = options.get('setterName')
        self._fieldName = options.get('fieldName')
        self._displayName = options.get('displayName')

        # format options
        self._stringFormat = options.get('stringFormat', '')
        self._enum = options.get('enum', None)

        # validation options
        self._validators = []

        # referencing options
        self._referenced = options.get('referenced', False)
        self._reference = options.get('reference', '')
        self._referenceRemovedAction = options.get('referenceRemovedAction',
                                                   RemovedAction.DoNothing)

        # reversed referencing options
        self._reversed = options.get('reversed', bool(options.get('reversedName')))
        self._reversedName = options.get('reversedName', '')
        self._reversedCached = options.get('reversedCached', False)
        self._reversedCacheTimeout = options.get('reversedCacheTimeout', options.get('reversedCachedExpires', 0))

        # indexing options
        self._indexed = options.get('indexed', False)
        self._indexCached = options.get('indexCached', False)
        self._indexName = options.get('indexName')
        self._indexCacheTimeout = options.get('indexCacheTimeout', options.get('indexCachedExpires', 0))

        # additional properties
        self._default = options.get('default', None)
        self._maxlength = options.get('maxlength', 0)
        self._joiner = options.get('joiner', None)
        self._aggregator = options.get('aggregator', None)

        # flags options
        flags = options.get('flags', 0)

        # by default, all columns are data columns
        if options.get('primary'):
            flags |= Column.Flags.Primary
        if options.get('private'):
            flags |= Column.Flags.Private
        if options.get('readOnly'):
            flags |= Column.Flags.ReadOnly
        if options.get('polymorphic'):
            flags |= Column.Flags.Polymorphic
        if options.get('autoIncrement'):
            flags |= Column.Flags.AutoIncrement
        if options.get('required', options.get('primary')):
            flags |= Column.Flags.Required
        if options.get('unique'):
            flags |= Column.Flags.Unique
        if options.get('encrypted', self._type == ColumnType.Password):
            flags |= Column.Flags.Encrypted
        if options.get('searchable'):
            flags |= Column.Flags.Searchable
        if options.get('ignoreByDefault'):
            flags |= Column.Flags.IgnoreByDefault
        if options.get('translatable'):
            flags |= Column.Flags.Translatable

        self._flags = flags

        # determine the kind of column that this column is
        if options.get('proxy'):
            self._kind = Column.Kind.Proxy
        elif self._joiner or self._shortcut:
            self._kind = Column.Kind.Joined
        elif self._aggregator:
            self._kind = Column.Kind.Aggregate
        else:
            self._kind = Column.Kind.Field

    def aggregate(self):
        """
        Returns the query aggregate that is associated with this column.
        
        :return     <orb.QueryAggregate> || None
        """
        if self._aggregator:
            return self._aggregator.generate(self)
        return None

    def aggregator(self):
        """
        Returns the aggregation instance associated with this column.  Unlike
        the <aggregate> function, this method will return the class instance
        versus the resulting <orb.QueryAggregate>.
        
        :return     <orb.ColumnAggregator> || None
        """
        return self._aggregator

    def autoIncrement(self):
        """
        Returns whether or not this column should 
        autoIncrement in the database.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.AutoIncrement)

    def columnType(self, baseOnly=False):
        """
        Returns the type of data that this column represents.
        
        :return     <orb.common.ColumnType>
        """
        if baseOnly:
            return ColumnType.base(self._type)
        return self._type

    def copy(self):
        """
        Creates a copy of this column and returns it.

        :return     <orb.Column>
        """
        return Column.fromXml(self.toXml())

    def columnTypeText(self, baseOnly=False):
        """
        Returns the column type text for this column.
        
        :return     <str>
        """
        return ColumnType[self.columnType(baseOnly=baseOnly)]

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

    def engine(self, db=None):
        """
        Returns the data engine for this column for the given database.
        Individual databases can define their own data engines.  If no database
        is defined, then the currently active database will be used.
        
        :param      db | <orb.Database> || None
        
        :return     <orb.ColumnEngine> || None
        """
        try:
            return self._engines[db]
        except KeyError:
            engine = None

            # lookup the current database
            if db is None:
                db = orb.system.database()

            # lookup the database engine for this instance
            if db:
                try:
                    engine = self._engines[db.databaseType()]
                except KeyError:
                    engine = db.columnEngine(self)

                self._engines[db] = engine

        return engine

    def default(self, resolve=False):
        """
        Returns the default value for this column to return
        when generating new instances.
        
        :return     <variant>
        """
        default = self._default
        ctype = ColumnType.base(self.columnType())

        if not resolve:
            return default

        elif default == 'None':
            return None

        elif ctype == ColumnType.Bool:
            if type(default) == bool:
                return default
            else:
                return nstr(default) in ('True', '1')

        elif ctype in (ColumnType.Integer,
                       ColumnType.Enum,
                       ColumnType.BigInt,
                       ColumnType.Double):

            if type(default) in (str, unicode):
                try:
                    return eval(default)
                except SyntaxError:
                    return 0
            elif default is None:
                return 0
            else:
                return default

        elif ctype == ColumnType.Date:
            if isinstance(default, datetime.date):
                return default
            elif default in ('today', 'now'):
                return datetime.date.today()
            return None

        elif ctype == ColumnType.Time:
            if isinstance(default, datetime.time):
                return default
            elif default == 'now':
                return datetime.datetime.now().time()
            else:
                return None

        elif ctype in (ColumnType.Datetime, ColumnType.Timestamp):
            if isinstance(default, datetime.datetime):
                return default
            elif default in ('today', 'now'):
                return datetime.datetime.now()
            else:
                return None

        elif ctype == (ColumnType.DatetimeWithTimezone, ColumnType.Timestamp_UTC):
            if isinstance(default, datetime.datetime):
                return default
            elif default in ('today', 'now'):
                return datetime.datetime.utcnow()
            else:
                return None

        elif ctype == ColumnType.Interval:
            if isinstance(default, datetime.timedelta):
                return default
            elif default == 'now':
                return datetime.timedelta()
            else:
                return None

        elif ctype == ColumnType.Decimal:
            if default is None:
                return decimal.Decimal()
            return default

        elif ctype & (ColumnType.String |
                      ColumnType.Text |
                      ColumnType.Url |
                      ColumnType.Email |
                      ColumnType.Password |
                      ColumnType.Filepath |
                      ColumnType.Directory |
                      ColumnType.Xml |
                      ColumnType.Html |
                      ColumnType.Color):
            if default is None:
                return ''
            return default

        else:
            return None

    def defaultOrder(self):
        """
        Returns the default ordering for this column based on its type.
        
        Strings will be desc first, all other columns will be asc.
        
        
        :return     <str>
        """
        if self.isString():
            return 'desc'
        return 'asc'

    def displayName(self, autoGenerate=True):
        """
        Returns the display name for this column - if no name is \
        explicitly set, then the words for the column name will be \
        used.
        
        :param      autoGenerate | <bool>
        
        :return     <str>
        """
        if not autoGenerate or self._displayName:
            return self._displayName

        return projex.text.capitalizeWords(self.name())

    def enum(self):
        """
        Returns the enumeration that is associated with this column.  This can
        help for automated validation when dealing with enumeration types.
        
        :return     <projex.enum.enum> || None
        """
        return self._enum

    def getter(self):
        """
        Returns the getter method linked with this column.  This is used in
        proxy columns.
        
        :return     <callable> || None
        """
        return self._getter

    def fieldName(self):
        """
        Returns the field name that this column will have inside
        the database.  The Column.TEMPLATE_FIELD variable will be
        used for this property by default.
                    
        :return     <str>
        """
        return self._fieldName

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

    def getterName(self):
        """
        Returns the name for the getter method that will be 
        generated for this column.  The Column.TEMPLATE_GETTER 
        variable will be used for this property by default.
        
        :return     <str>
        """
        return self._getterName

    def indexed(self):
        """
        Returns whether or not this column is indexed for quick
        lookup.
        
        :return     <bool>
        """
        return self._indexed

    def indexCached(self):
        """
        Returns whether or not the index for this column should cache the
        records.
        
        :return     <bool>
        """
        return self._indexCached

    def indexCacheTimeout(self):
        """
        Returns the time in seconds for how long to store a client side cache
        of the results for the index on this column.
        
        :return     <int> | seconds
        """
        return self._indexCacheTimeout

    def indexName(self):
        """
        Returns the name to be used when generating an index
        for this column.
        
        :return     <str>
        """
        return self._indexName

    def isAggregate(self):
        """
        Returns whether or not this column is a aggregate.
        
        :return     <bool>
        """
        return self._aggregator is not None

    def isEncrypted(self):
        """
        Returns whether or not the data in this column should be encrypted.
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Encrypted)

    def isInteger(self):
        """
        Returns whether or not this column is an integer.
        
        :return     <bool>
        """
        btype = ColumnType.base(self.columnType())
        return btype in (ColumnType.Integer, ColumnType.BigInt, ColumnType.Enum)

    def isKind(self, kind):
        """
        Returns whether or not this column is the kind of inputted type.

        :param      kind | <orb.Column.Kind>

        :return     <bool>
        """
        return bool(self._kind & kind) if kind >= 0 else not bool(self._kind & ~kind)

    def isJoined(self):
        """
        Returns whether or not this column is a joined column.  Dynamic
        columns are not actually a part of the database table, but rather
        joined in data using a query during selection.
        
        :return     <bool>
        """
        return self._joiner is not None

    def isMatch(self, name):
        """
        Returns whether or not this column's text info matches the inputted name.
        
        :param      name | <str>
        """
        if name == self:
            return True

        opts = (self.name(),
                self.name().strip('_'),
                projex.text.camelHump(self.name()),  # support both styles for string lookup
                projex.text.underscore(self.name()),
                self.displayName(),
                self.fieldName())

        return name in opts

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

    def isPolymorphic(self):
        """
        Returns whether or not this column defines the polymorphic class
        for the schema.  If this is set to True in conjunction with a
        reference model, the text for the reference model will be used
        to try to inflate the record to its proper class.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Polymorphic)

    def isPrivate(self):
        """
        Returns whether or not this column should be treated as private.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Private)

    def isProxy(self):
        """
        Returns whether or not this column is a proxy column.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.isKind(Column.Kind.Proxy)

    def isReadOnly(self):
        """
        Returns whether or not this column is read-only.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.ReadOnly)

    def isReference(self):
        """
        Returns whether or not this column is a reference to another table.
        
        :return     <bool>
        """
        if self._reference:
            return True
        return False

    def isReferenced(self):
        """
        Returns whether or not this column is referenced from an external file.
        
        :return     <bool>
        """
        return self._referenced

    def isReversed(self):
        """
        Returns whether or not this column generates a reverse lookup method \
        for its reference model.
        
        :return     <bool>
        """
        return self._reversed

    def isSearchable(self):
        """
        Returns whether or not this column is a searchable column.  If it is,
        when the user utilizes the search function for a record set, this column
        will be used as a matchable entry.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Searchable)

    def isString(self):
        """
        Returns whether or not this column is of a string type.
        
        :return     <bool>
        """
        string_types = ColumnType.String
        string_types |= ColumnType.Text
        string_types |= ColumnType.Url
        string_types |= ColumnType.Email
        string_types |= ColumnType.Password
        string_types |= ColumnType.Filepath
        string_types |= ColumnType.Directory
        string_types |= ColumnType.Xml
        string_types |= ColumnType.Html
        string_types |= ColumnType.Color

        return (ColumnType.base(self.columnType()) & string_types) != 0

    def isTranslatable(self):
        """
        Returns whether or not this column is translatable.
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Translatable)

    def iterFlags(self):
        """
        Returns the flags that are currently set for this instance.
        
        :return     [<Column.Flags>, ..]
        """
        return [flag for flag in Column.Flags.values() if self.testFlag(flag)]

    def kind(self):
        """
        Returns the general kind of column this is.

        :return     <orb.Column.Kind>
        """
        return self._kind

    def joiner(self):
        """
        Returns the joiner query that is used to define what this columns
        value will be.
        
        :return     (<orb.Column>, <orb.Query>) || None
        """
        joiner = self._joiner

        # dynamically generate a join query based on the inputted function
        if type(joiner).__name__ == 'function':
            return joiner(self)

        # otherwise, if there is a shortcut, generate that
        else:
            return orb.Query(self.shortcut())

    def maxlength(self):
        """
        Returns the max length for this column.  This property
        is used for the varchar data type.
        
        :return     <int>
        """
        return self._maxlength

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

    def primary(self):
        """
        Returns if this column is one of the primary keys for the
        schema.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Primary)

    def reference(self):
        """
        Returns the model that this column is related to when
        it is a foreign key.
                    
        :return     <str>
        """
        return self._reference

    def referenceRemovedAction(self):
        """
        Determines how records for this column will act when the reference it
        points to is removed.
        
        :return     <ReferencedAction>
        """
        return self._referenceRemovedAction

    def referenceModel(self):
        """
        Returns the model that this column references.
        
        :return     <Table> || None
        """
        if not self.isReference():
            return None

        dbname = self.schema().databaseName() or None
        model = orb.system.model(self.reference(), database=dbname)
        if not model:
            raise errors.TableNotFound(self.reference())
        return model

    def restoreValue(self, value, options=None):
        """
        Restores the value from a table cache for usage.
        
        :param      value | <variant>
                    options | <orb.ContextOptions> || None
        """
        coltype = ColumnType.base(self.columnType())

        # always allow NULL types
        if value is None:
            return value

        # restore a datetime timezone value
        if isinstance(value, datetime.datetime) and \
           coltype == ColumnType.DatetimeWithTimezone:
            tz = self.timezone(options)

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

        # restore a timestamp value
        elif coltype in (ColumnType.Timestamp, ColumnType.Timestamp_UTC) and isinstance(value, (int, float)):
            value = datetime.datetime.fromtimestamp(value)

        # restore a string value
        elif self.isString():
            value = projex.text.decoded(value)

        return value

    def required(self):
        """
        Returns whether or not this column is required when
        creating records in the database.
        
        :sa         testFlag
        
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Required)

    def returnType(self):
        """
        Defines the return class type name that will be expected for
        this column type.
        """
        typ = '<variant>'
        if self.columnType() == ColumnType.Bool:
            typ = '<bool>'
        elif self.columnType() in (ColumnType.Decimal, ColumnType.Double):
            typ = '<float>'
        elif self.columnType() in (ColumnType.Integer, ColumnType.BigInt):
            typ = '<int>'
        elif self.columnType() == ColumnType.Enum:
            typ = '<int> (enum)'
        elif self.columnType() in (ColumnType.Datetime, ColumnType.Timestamp, ColumnType.Timestamp_UTC):
            typ = '<datetime.datetime>'
        elif self.columnType() == ColumnType.Date:
            typ = '<datetime.date> (without tz_info)'
        elif self.columnType() == ColumnType.Interval:
            typ = '<datetime.timedelta>'
        elif self.columnType() == ColumnType.Time:
            typ = '<datetime.time>'
        elif self.columnType() == ColumnType.DatetimeWithTimezone:
            typ = '<datetime.datetime> (with tz_info)'
        elif self.columnType() == ColumnType.Image:
            typ = '<unicode> (image bytea)'
        elif self.columnType() == ColumnType.ByteArray:
            typ = '<bytea>'
        elif self.columnType() == ColumnType.Dict:
            typ = '<dict>'
        elif self.columnType() == ColumnType.Pickle:
            typ = '<variant> (pickle data)'
        elif self.columnType() == ColumnType.Query:
            typ = '<orb.query.Query>'
        elif self.columnType() == ColumnType.ForeignKey:
            typ = '<orb.schema.dynamic.{0}> || <variant> (primary key when not inflated)'.format(self.reference())
        elif self.isString():
            typ = '<unicode>'

        if not self.required():
            typ += ' || None'

        return typ

    def reversedCached(self):
        """
        Returns whether or not the reverse lookup for this column should
        be cached.
        
        :return     <bool>
        """
        return self._reversedCached

    def reversedCacheTimeout(self):
        """
        Returns the time in seconds that the cache should expire within.  If
        the value is 0, then the cache will never expire
        
        :return     <int> | seconds
        """
        return self._reversedCacheTimeout

    def reversedName(self):
        """
        Returns the name that will be used when generating a reverse accessor \
        for its reference table.
        
        :return     <str>
        """
        return self._reversedName

    def shortcut(self):
        """
        Returns the shortcut for this column.  This will traverse a relationship set
        for the actual value as a join from another table.

        :return     <str>
        """
        return self._shortcut

    def storeValue(self, value):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary
        
        :param      value | <variant>
        
        :return     <variant>
        """
        coltype = ColumnType.base(self.columnType())

        # ensure we have a value
        if isinstance(value, (str, unicode)):
            value = self.valueFromString(value)

        if value is None:
            return value

        # store timezone information
        elif coltype == ColumnType.DatetimeWithTimezone:
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

        # store timestamp information
        elif coltype in (ColumnType.Timestamp, ColumnType.Timestamp_UTC):
            if isinstance(value, datetime.datetime):
                value = time.mktime(value.timetuple())

        # encrypt the value if necessary
        elif self.isEncrypted():
            return orb.system.encrypt(value)

        # store non-html content
        elif self.isString():
            if bleach:
                value = bleach.clean(value)

            if coltype != ColumnType.Html and html_tools:
                value = html_tools.strip_tags(value)

        return value

    def schema(self):
        """
        Returns the table that this column is linked to in the database.
        
        :return     <TableSchema>
        """
        return self._schema

    def setterName(self):
        """
        Returns the setter name that will be used to generate the
        setter method on the table as it is generated.  The
        Column.TEMPLATE_SETTER property will be used by default.
        
        :return     <str>
        """
        return self._setterName

    def setter(self):
        """
        Returns the setter method linked with this column.  This is used in
        proxy columns.
        
        :return     <callable> || None
        """
        return self._setter

    def setAutoIncrement(self, state):
        """
        Sets whether or not this column should auto increment.
        
        :sa         setFlag
        
        :param      state | <bool>
        """
        self.setFlag(Column.Flags.AutoIncrement, state)

    def setColumnType(self, columnType):
        """
        Sets the column type that this column represents in the database.
        
        :param      columnType | <ColumnType>
        """
        self._type = columnType

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

    def setDisplayName(self, displayName):
        """
        Sets the display name for this column.
        
        :param      displayName | <str>
        """
        if displayName is not None:
            self._displayName = displayName

    def setEngine(self, db_or_type, engine):
        """
        Sets the database engine for this column in the given database.
        
        :param      db_or_type | <orb.Database> || <str>
                    engine     | <orb.ColumnEngine>
        """
        self._engines[db_or_type] = engine

    def setEnum(self, cls):
        """
        Sets the enumeration that is associated with this column to the inputted
        type.  This is an optional parameter but can be useful when dealing
        with validation and some of the automated features of the ORB system.
        
        :param      cls | <projex.enum.enum> || None
        """
        self._enum = cls

    def setEncrypted(self, state):
        """
        Sets whether or not this column is encrypted in the database.
        
        :sa         setFlag
        
        :param      state   | <bool>
        """
        self.setFlag(Column.Flags.Encrypted, state)

    def setFieldName(self, fieldName):
        """
        Sets the field name for this column.
        
        :param      fieldName | <str>
        """
        if fieldName is not None:
            self._fieldName = fieldName

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

    def setGetterName(self, getterName):
        """
        Sets the getter name for this column.
        
        :param      getterName | <str>
        """
        if getterName is not None:
            self._getterName = getterName

    def setIndexed(self, state):
        """
        Sets whether or not this column will create a lookup index.
        
        :param      state   | <bool>
        """
        self._indexed = state

    def setIndexCached(self, cached):
        """
        Sets whether or not the index should cache the results from the
        database when looking up this column.
        
        :param      cached | <bool>
        """
        self._indexCached = cached

    def setIndexCacheTimeout(self, seconds):
        """
        Sets the time in seconds for how long to store a client side cache
        of the results for the index on this column.
        
        :param     seconds | <int>
        """
        self._indexCacheTimeout = seconds

    def setIndexName(self, indexName):
        """
        Sets the index name for this column.
        
        :param      indexName | <str>
        """
        if indexName is not None:
            self._indexName = indexName

    def setJoiner(self, joiner):
        """
        Sets the joiner query for this column to the inputted query.
        
        :param      query | (<orb.Column>, <orb.Query>) || <callable> || None
        """
        self._joiner = joiner
        if joiner is not None:
            self._kind = Column.Kind.Joined

            self.setFlag(Column.Flags.ReadOnly)
            self.setFlag(Column.Flags.Field, False)  # joiner columns are not fields

    def setMaxlength(self, length):
        """
        Sets the maximum length for this column.  Used when defining string \
        column types.
        
        :param      length | <int>
        """
        self._maxlength = length

    def setPolymorphic(self, state):
        """
        Sets whether or not this column defines a polymorphic mapper for
        the table.
        
        :sa         setFlag
        
        :param      state | <bool>
        """
        self.setFlag(Column.Flags.Polymorphic, state)

    def setPrimary(self, primary):
        """
        Sets whether or not this column is one of the primary columns \
        for a table.
        
        :sa         setFlag
        
        :param      primary     | <bool>
        """
        self.setFlag(Column.Flags.Primary, primary)

    def setPrivate(self, state):
        """
        Sets whether or not this column should be treated as a private column.
        
        :sa         setFlag
        
        :param      state | <bool>
        """
        self.setFlag(Column.Flags.Private, state)

    def setAggregator(self, aggregator):
        """
        Sets the query aggregate for this column to the inputted aggregate.
        
        :param      aggregator | <orb.ColumnAggregator> || None
        """
        self._aggregator = aggregator

        # defines this column as an aggregation value
        # (does not explicitly live on the Table class)
        if aggregator is not None:
            self._kind = Column.Kind.Aggregate
            self.setFlag(Column.Flags.ReadOnly)

    def setReadOnly(self, state):
        """
        Sets whether or not this column is a read only attribute.
        
        :sa         setFlag
        
        :param      state | <bool>
        """
        self.setFlag(Column.Flags.ReadOnly, state)

    def setReference(self, reference):
        """
        Sets the name of the table schema instance that this column refers \
        to from its schema.
        
        :param      reference       | <str>
        """
        self._reference = reference

    def setReferenceRemovedAction(self, referencedAction):
        """
        Sets how records for this column will act when the reference it
        points to is removed.
        
        :param     referencedAction | <ReferencedAction>
        """
        self._referenceRemovedAction = referencedAction

    def setRequired(self, required):
        """
        Sets whether or not this column is required in the database.
        
        :sa         setFlag
        
        :param      required | <bool>
        """
        self.setFlag(Column.Flags.Required, required)

    def setReversed(self, state):
        """
        Sets whether or not this column generates a reverse accessor for \
        lookups to its reference table.
        
        :param      state | <bool>
        """
        self._reversed = state

    def setReversedCached(self, state):
        """
        Sets whether or not the reverse lookup for this column should be
        cached.
        
        :param      state | <bool>
        """
        self._reversedCached = state

    def setReversedCacheTimeout(self, seconds):
        """
        Sets the time in seconds that the cache will remain on the client side
        before needing to request an update from the server.  If the seconds
        is 0, then the cache will never expire.
        
        :param      seconds | <int>
        """
        self._reversedCacheTimeout = seconds

    def setReversedName(self, reversedName):
        """
        Sets the reversing name for the method that will be generated for the \
        lookup to its reference table.
        
        :param      reversedName | <str>
        """
        if reversedName is not None:
            self._reversedName = reversedName

    def setSearchable(self, state):
        """
        Sets whether or not this column is used during record set searches.
        
        :sa         setFlag
        
        :param      state | <bool>
        """
        self.setFlag(Column.Flags.Searchable, state)

    def setSetterName(self, setterName):
        """
        Sets the setter name for this column.
        
        :param      setterName | <str>
        """
        if setterName is not None:
            self._setterName = setterName

    def setShortcut(self, shortcut):
        """
        Sets the shortcut path for this column.

        :param      shortcut | <str>
        """
        self._shortcut = shortcut
        if self._joiner or shortcut:
            self._kind = Column.Kind.Joined
        elif self._kind == Column.Kind.Joined:
            self._kind = Column.Kind.Field

    def setStringFormat(self, formatter):
        """
        Sets the string formatter for this column to the inputted text.  This
        will use Python's string formatting system to format values for the
        column when it is displaying its value.
        
        :param      formatter | <str>
        """
        self._stringFormat = formatter

    def setTimezone(self, timezone):
        """
        Sets the timezone associated directly to this column.
        
        :sa     <orb.Manager.setTimezone>
        
        :param     timezone | <pytz.tzfile> || None
        """
        self._timezone = timezone

    def setTranslatable(self, state):
        """
        Returns whether or not this column is translatable.
        
        :return     <bool>
        """
        self.setFlag(Column.Flags.Translatable, state)

    def setUnique(self, state):
        """
        Sets whether or not this column is unique in the database.
        
        :param      setFlag
        
        :param      unique | <bool>
        """
        self.setFlag(Column.Flags.Unique, state)

    def stringFormat(self):
        """
        Returns the string formatter for this column to the inputted text.  This
        will use Python's string formatting system to format values for the
        column when it is displaying its value.
        
        :return     <str>
        """
        return self._stringFormat

    def testFlag(self, flag):
        """
        Tests to see if this column has the inputted flag set.
        
        :param      flag | <Column.Flags>
        """
        return bool(self.flags() & flag) if flag >= 0 else not bool(self.flags() & ~flag)

    def timezone(self, options=None):
        """
        Returns the timezone associated specifically with this column.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the system in general.
        
        :sa     <orb.Manager>

        :param      options | <orb.ContextOptions> || None
        
        :return     <pytz.tzfile> || None
        """
        if self._timezone is None:
            return self.schema().timezone(options)
        return self._timezone

    def toolTip(self, context='normal'):
        example_values = {
            'ForeignKey': '{0}.all().first()'.format(self.reference()),
            'Integer': '0',
            'String': '"example string"',
            'Text': '"example text"',
            'Bool': 'True',
            'Email': 'me@example.com',
            'Password': 'ex@mp1e',
            'BigInt': '10',
            'Date': 'datetime.date.today()',
            'Datetime': 'datetime.datetime.now()',
            'DatetimeWithTimezone': 'datetime.datetime.now()',
            'Time': 'datetime.time(12, 0, 0)',
            'Timestamp': 'datetime.datetime.now()',
            'Timestamp_UTC': 'datetime.datetime.now()',
            'Interval': 'datetime.timedelta(seconds=1)',
            'Decimal': '0.0',
            'Double': '0.0',
            'Enum': 'Types.Example',
            'Url': '"http://example.com"',
            'Filepath': '"/path/to/example.png"',
            'Directory': '"/path/to/example/"',
            'Xml': '"<example></example>"',
            'Html': '"<html><example></example></html>"',
            'Color': '"#000"',
            'Image': 'QtGui.QPixmap("/path/to/example.png")',
            'ByteArray': 'QtCore.QByteArray()',
            'Pickle': '{"example": 10}',
            'Dict': '{"example": 10}',
            'Query': 'orb.Query(column) == value'
        }

        example_returns = example_values.copy()
        example_returns['ForeignKey'] = '&lt;{0}&gt;'.format(self.reference())
        example_returns['Query'] = '&lt;orb.Query&gt;'

        coltype = self.columnTypeText()
        default = projex.text.underscore(self.name())
        opts = {'name': self.name(),
                'type': self.reference() if coltype == 'ForeignKey' else self.columnTypeText(),
                'field': self.fieldName(),
                'getter': self.getterName(),
                'setter': self.setterName(),
                'schema': self.schema().name(),
                'display': self.displayName(),
                'value': example_values.get(coltype, default),
                'default': default,
                'return': example_returns.get(coltype, example_values.get(coltype, default)),
                'record': projex.text.underscore(self.schema().name()),
                'ref_record': projex.text.underscore(self.reference())}

        # show indexed information
        if context == 'normal':
            tip = '''\
<b>{schema}.{name} <small>({type})</small></b>
<pre>
>>> # example api usage
>>> {record} = {schema}()
>>> {record}.{setter}({value})
>>> {record}.{getter}()
{return}

>>> # meta data
>>> column = {schema}.schema().column('{name}')

>>> # ui display info
>>> column.displayName()
'{display}'

>>> # database field info
>>> column.fieldName()
'{field}'
</pre>'''

        elif context == 'index' and self.indexed():
            opts['index'] = self.indexName()
            if self.unique():
                tip = '''\
<b>{schema}.{name} <small>({schema} || None)</small></b>
<pre>
>>> # lookup record by index
>>> {schema}.{index}({default})
&lt;{schema}&gt;
</pre>
'''
            else:
                tip = '''\
<b>{schema}.{name} <small>(RecordSet([{schema}, ..]))</small></b>
<pre>
>>> # lookup records by index
>>> {schema}.{index}({default})
&lt;orb.RecordSet([&lt;{schema}&gt;, ..])&gt;
</pre>
'''

        # show reversed information
        elif context == 'reverse' and self.isReversed():
            opts['reference'] = self.reference()
            opts['reverse'] = self.reversedName()

            if self.unique():
                tip = '''\
<b>{reference}.{reverse} <small>({reference})</small></b><br>
<pre>
>>> # look up {schema} record through the reverse accessor
>>> {ref_record} = {reference}()
>>> {ref_record}.{reverse}()
&lt;{schema}&gt;
</pre>
'''
            else:
                tip = '''\
<b>{reference}.{reverse} <small>(RecordSet([{reference}, ..]))</small></b><br>
<pre>
>>> # look up {schema} records through the reverse accessor
>>> {ref_record} = {reference}()
>>> {ref_record}.{reverse}()
&lt;orb.RecordSet([&lt;{schema}&gt;, ..])&gt;
</pre>
'''

        else:
            tip = ''

        return tip.format(**opts)

    def toXml(self, xparent=None):
        """
        Saves the data about this column out to xml as a child node for the
        inputted parent.
        
        :param      xparent    | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        if xparent is not None:
            xcolumn = ElementTree.SubElement(xparent, 'column')
        else:
            xcolumn = ElementTree.Element('column')

        # save the properties
        xcolumn.set('name', self.name())

        # store as elements
        ElementTree.SubElement(xcolumn, 'type').text = ColumnType[self.columnType()]
        ElementTree.SubElement(xcolumn, 'display').text = self.displayName(False)
        ElementTree.SubElement(xcolumn, 'getter').text = self.getterName()
        ElementTree.SubElement(xcolumn, 'setter').text = self.setterName()
        ElementTree.SubElement(xcolumn, 'field').text = self._fieldName

        if self._shortcut:
            ElementTree.SubElement(xcolumn, 'shortcut').text = self._shortcut

        # store string format options
        if self._stringFormat:
            ElementTree.SubElement(xcolumn, 'format').text = self._stringFormat

        if self.default():
            ElementTree.SubElement(xcolumn, 'default').text = nstr(self.default())

        # store additional options
        if self.maxlength():
            ElementTree.SubElement(xcolumn, 'maxlen').text = nstr(self.maxlength())

        # store indexing options
        if self.indexed():
            xindex = ElementTree.SubElement(xcolumn, 'index')
            xindex.text = self.indexName()
            if self.indexCached():
                xindex.set('cached', nstr(self.indexCached()))
                xindex.set('timeout', nstr(self.indexCacheTimeout()))

        # store flags
        xflags = ElementTree.SubElement(xcolumn, 'flags')
        for flag in Column.Flags.keys():
            has_flag = self.testFlag(Column.Flags[flag])
            if has_flag:
                ElementTree.SubElement(xflags, flag)

        # store referencing options
        if self.reference():
            xrelation = ElementTree.SubElement(xcolumn, 'relation')
            ElementTree.SubElement(xrelation, 'table').text = self.reference()
            ElementTree.SubElement(xrelation, 'removedAction').text = nstr(self.referenceRemovedAction())

            if self.isReversed():
                xreversed = ElementTree.SubElement(xrelation, 'reversed')
                xreversed.text = self.reversedName()

                if self.reversedCached():
                    xreversed.set('cached', nstr(self.reversedCached()))
                    xreversed.set('timeout', nstr(self.reversedCacheTimeout()))

        return xcolumn

    def unique(self):
        """
        Returns whether or not this column should be unique in the
        database.
                    
        :return     <bool>
        """
        return self.testFlag(Column.Flags.Unique)

    def validate(self, value):
        """
        Validates the inputted value against this columns rules.  If the inputted value does not pass, then
        a validation error will be raised.
        
        :param      value | <variant>
        
        :return     <bool> success
        """
        for validator in self.validators():
            validator.validate(self, value)
        return True

    def validators(self):
        """
        Returns the regular expression pattern validator for this column.
        
        :return     [<orb.Validator>, ..]
        """
        default = []
        if self.required() and not self.autoIncrement():
            default.append(orb.RequiredValidator())

        return self._validators + default

    def valueFromString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :param      value | <str>
                    extra | <variant>
        """
        # convert the value from a string value via the data engine system
        engine = self.engine(db)
        if engine:
            return engine.fromString(value)

        # convert the value to a string using default values
        coltype = ColumnType.base(self.columnType())
        if coltype == ColumnType.Date:
            if dateutil_parser:
                return dateutil_parser.parse(value).date()
            else:
                extra = extra or '%Y-%m-%d'
                time_struct = time.strptime(value, extra)
                return datetime.date(time_struct.tm_year,
                                     time_struct.tm_month,
                                     time_struct.tm_day)

        elif coltype == ColumnType.Time:
            if dateutil_parser:
                return dateutil_parser.parse(value).time()
            else:
                extra = extra or '%h:%m:%s'
                time_struct = time.strptime(value, extra)
                return datetime.time(time_struct.tm_hour,
                                     time_struct.tm_min,
                                     time_struct.tm_sec)

        elif coltype in (ColumnType.Datetime, ColumnType.DatetimeWithTimezone):
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

        elif coltype in (ColumnType.Timestamp, ColumnType.Timestamp_UTC):
            try:
                return datetime.datetime.fromtimestamp(float(value))
            except StandardError:
                if dateutil_parser:
                    return dateutil_parser.parse(value)
                else:
                    return datetime.datetime.min()

        elif coltype == ColumnType.Bool:
            return nstr(value).lower() == 'true'

        elif coltype in (ColumnType.Integer,
                         ColumnType.Double,
                         ColumnType.Decimal,
                         ColumnType.BigInt,
                         ColumnType.Enum):
            try:
                value = projex.text.safe_eval(value)
            except ValueError:
                value = 0

            return value

        return nstr(value)

    def valueToString(self, value, extra=None, db=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :sa         engine
        
        :param      value | <str>
                    extra | <variant>
        """
        # convert the value to a string value via the data engine system
        engine = self.engine(db)
        if engine:
            return engine.toString(value)

        # convert the value to a string using default values
        coltype = ColumnType.base(self.columnType())

        if coltype == ColumnType.Date:
            if extra is None:
                extra = '%Y-%m-%d'

            return value.strftime(extra)

        elif coltype == ColumnType.Time:
            if extra is None:
                extra = '%h:%m:%s'

            return value.strftime(extra)

        elif coltype == ColumnType.Datetime:
            if extra is None:
                extra = '%Y-%m-%d %h:%m:s'

            return value.strftime(extra)

        return nstr(value)

    @staticmethod
    def defaultDatabaseName(typ, name, reference=''):
        """
        Returns the default schema name based on the current column templates.
        
        :param      typ         |  <str>
                    name        |  <str>
                    reference   | <str> | table the column refers to
        
        :return     <str>
        """
        # make sure we have an actual name to process
        name = nstr(name).strip()
        if not name:
            return ''

        # generate a reference field type
        if typ == 'fieldName' and reference:
            templ = Column.TEMPLATE_REFERENCE

        # generate the default templates
        else:
            templ = Column.TEMPLATE_MAP.get(typ)

        if templ is None:
            return ''

        return projex.text.render(templ, {'name': name, 'table': reference})

    @staticmethod
    def defaultPrimaryColumn(name):
        """
        Creates a default primary column based on the inputted table schema.
        
        :return     <Column>
        """

        # generate the auto column
        fieldName = Column.defaultDatabaseName('primaryKey', name)
        column = Column(ColumnType.Integer, 'id')
        column.setPrimary(True)
        column.setFieldName(fieldName)
        column.setAutoIncrement(True)
        column.setRequired(True)
        column.setUnique(True)

        return column

    @staticmethod
    def fromXml(xcolumn, referenced=False):
        """
        Generates a new column from the inputted xml column data.
        
        :param      xcolumn | <xml.etree.Element>
        
        :return     <Column> || None
        """
        try:
            typ = xcolumn.find('type').text
        except AttributeError:
            typ = xcolumn.get('type')

        try:
            typ = ColumnType[typ]
        except KeyError:
            raise orb.errors.InvalidColumnType(typ)

        name = xcolumn.get('name')
        if typ is None or not name:
            return None

        # create the column
        column = Column(typ, name, referenced=referenced)

        try:  # as of orb 4.3
            column.setGetterName(xcolumn.find('getter').text)
        except AttributeError:
            column.setGetterName(xcolumn.get('getter'))

        try:
            column.setSetterName(xcolumn.find('setter').text)
        except AttributeError:
            column.setSetterName(xcolumn.get('setter'))

        try:
            column.setFieldName(xcolumn.find('field').text)
        except AttributeError:
            column.setFieldName(xcolumn.get('field'))

        try:
            column.setDisplayName(xcolumn.find('display').text)
        except AttributeError:
            column.setDisplayName(xcolumn.get('display'))

        try:
            column.setShortcut(xcolumn.find('shortcut').text)
        except AttributeError:
            column.setShortcut(xcolumn.get('shortcut', ''))

        try:
            column.setDefault(xcolumn.find('default').text)
        except AttributeError:
            column.setDefault(xcolumn.get('default', None))

        try:
            column.setDefault(int(xcolumn.find('maxlen').text))
        except AttributeError:
            maxlen = xcolumn.get('maxlen')
            if maxlen is not None:
                column.setMaxlength(int(maxlen))

        # restore formatting options
        try:
            column._stringFormat = xcolumn.find('format').text
        except AttributeError:
            pass

        # restore the flag information
        flags = 0
        xflags = xcolumn.find('flags')
        if xflags is not None:
            for xchild in xflags:
                try:
                    flags |= Column.Flags[projex.text.classname(xchild.tag)]
                except KeyError:
                    log.error('{0} is not a valid column flag.'.format(xchild.tag))
        else:
            for flag in Column.Flags.keys():
                state = xcolumn.get(flag[0].lower() + flag[1:]) == 'True'
                if state:
                    try:
                        flags |= Column.Flags[projex.text.classname(flag)]
                    except KeyError:
                        log.error('{0} is not a valid column flag.'.format(flag))

        column.setFlags(flags)

        # restore the index information
        xindex = xcolumn.find('index')
        if xindex is not None:  # as of 4.3
            column.setIndexName(xindex.text)
            column.setIndexed(True)
            column.setIndexCached(xindex.get('cached') == 'True')
            column.setIndexCacheTimeout(int(xcolumn.get('timeout',
                                                        xcolumn.get('expires', column.indexCacheTimeout()))))
        else:
            # restore indexing options
            column.setIndexName(xcolumn.get('index'))
            column.setIndexed(xcolumn.get('indexed') == 'True')
            column.setIndexCached(xcolumn.get('indexCached') == 'True')
            column.setIndexCacheTimeout(int(xcolumn.get('indexCachedExpires',
                                                        column.indexCacheTimeout())))

        # create relation information
        xrelation = xcolumn.find('relation')
        if xrelation is not None:
            try:
                column.setReference(xrelation.find('table').text)
            except AttributeError:
                column.setReference(xrelation.get('table', ''))

            try:
                action = int(xrelation.find('removedAction').text)
            except StandardError:
                action = int(xrelation.get('removedAction', 1))

            column.setReferenceRemovedAction(action)

            xreversed = xrelation.find('reversed')
            if xreversed is not None:
                column.setReversed(True)
                column.setReversedName(xreversed.text)
                column.setReversedCached(xreversed.get('cached') == 'True')
                column.setReversedCacheTimeout(int(xreversed.get('timeout',
                                                                 xreversed.get('expires',
                                                                               column.reversedCacheTimeout()))))
            else:
                column.setReversed(xrelation.get('reversed') == 'True')
                column.setReversedName(xrelation.get('reversedName'))
                column.setReversedCached(xrelation.get('cached') == 'True')
                column.setReversedCacheTimeout(int(xrelation.get('expires', column.reversedCacheTimeout())))

        return column