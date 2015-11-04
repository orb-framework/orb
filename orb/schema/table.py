"""
Defines the main Table class that will be used when developing
database classes.
"""

import datetime
import logging
import projex.rest
import projex.security
import projex.text
import re

from projex.enum import enum
from projex.locks import ReadLocker, ReadWriteLock, WriteLocker
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from projex.callbacks import CallbackSet

from .meta.metatable import MetaTable
from ..common import ColumnType
from ..querying import Query as Q

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')

# treat unicode warnings as errors
from exceptions import UnicodeWarning
from warnings import filterwarnings

filterwarnings(action='error', category=UnicodeWarning)


class Table(object):
    """ 
    Defines the base class type that all database records should inherit from.
    """
    # define the table meta class
    __metaclass__ = MetaTable

    # meta database information
    __db__ = ''  # name of DB in Environment for this model
    __db_group__ = 'Default'  # name of database group
    __db_name__ = ''  # name of database schema
    __db_tablename__ = ''  # name of table in database
    __db_schema__ = None  # <orb.TableSchema> for direct creation
    __db_ignore__ = True  # bypass database table processing
    __db_archive__ = False
    __db_implements__ = None

    ReloadOptions = enum('Conflicts',
                         'Modified',
                         'Unmodified',
                         'IgnoreConflicts')

    Signals = enum(
        AboutToCommit='aboutToCommit(Record,LookupOptions,ContextOptions)',
        CommitFinished='commitFinished(Record,LookupOptions,ContextOptions)'
    )

    # ----------------------------------------------------------------------
    # PRIVATE CLASS METHODS
    #----------------------------------------------------------------------

    @classmethod
    def __syncdatabase__(cls):
        """
        This method will be called after all creation and updating syncing
        is called for a database sync.  It will allow a developer to specify
        default information for their class type.
        """
        pass

    #----------------------------------------------------------------------
    #                       PRIVATE STATIC METHODS
    #----------------------------------------------------------------------

    @staticmethod
    def __getKeywords(func):
        """
        Parses the keywords from the given function.
        
        :param      func | <function>
        """
        if hasattr(func, 'im_func'):
            func = func.im_func

        try:
            return func.func_code.co_varnames[-len(func.func_defaults):]
        except (TypeError, ValueError, IndexError):
            return tuple()

    @staticmethod
    def __groupingKey(record,
                      schema,
                      grouping,
                      ref_cache,
                      inflated=False,
                      locale=None):
        """
        Looks up the grouping key for the inputted record.  If the cache
        value is specified, then it will lookup any reference information within
        the cache and return it.
        
        :param      record | <orb.Table>
                    grouping | <str>
                    cache    | <dict> || None
                    inflated | <bool>
        
        :return     <str>
        """
        columnName = grouping

        # lookup template patterns
        if '[' in columnName:
            pattern = re.compile('\[([^\]:]+[^\]]*)\]')
            columnNames = pattern.findall(columnName)
            syntax = columnName
        else:
            columnNames = [columnName]
            syntax = None

        column_data = {}
        for columnName in columnNames:
            columnName = columnName.split(':')[0]
            column = schema.column(columnName)

            if not column:
                log.warning('%s is not a valid column of %s',
                            columnName,
                            schema.name())
                continue

            # lookup references
            if column.isReference():
                ref_key = record.recordValue(columnName,
                                             inflated=False,
                                             locale=locale)
                ref_cache_key = (column.reference(), ref_key)

                # cache this record so that we only access 1 of them
                if ref_cache_key not in ref_cache:
                    col_value = record.recordValue(columnName,
                                                   inflated=inflated,
                                                   locale=locale)
                    ref_cache[ref_cache_key] = col_value
                else:
                    col_value = ref_cache[ref_cache_key]
            else:
                col_value = record.recordValue(columnName, locale=locale)

            column_data[columnName] = col_value

        if syntax:
            return projex.text.render(syntax, column_data)
        else:
            return column_data[columnName]

    #----------------------------------------------------------------------

    def __reduce__(self):
        return type(self), (self.__getstate__(),)

    def __getstate__(self):
        state = {
            '__pickle': {
                'defaults': {k.name(): v for k, v in self.__record_defaults.items()},
                'values': {k.name(): v for k, v in self.__record_values.items()},
                'dbloaded': {x.name() for x in self.__record_dbloaded},
                'lookup': dict(self.__lookup_options) if self.__context_options else None,
                'context': dict(self.__context_options) if self.__context_options else None
            }
        }
        return state

    def __setstate__(self, state):
        schema = self.schema()

        self.__record_defaults = {schema.column(k): v for k, v in state.get('defaults', {}).items()}
        self.__record_values = {schema.column(k): v for k, v in state.get('values', {}).items()}
        self.__record_dbloaded = {schema.column(x) for x in state.get('dbloaded', [])}

        self.__lookup_options = orb.LookupOptions(**state.get('lookup')) if state.get('lookup') else None
        self.__context_options = orb.ContextOptions(**state.get('context')) if state.get('context') else None

    def __getitem__(self, key):
        column = self.schema().column(key)
        if column:
            return self.recordValue(column)
        else:
            raise KeyError(key)

    def __json__(self, *args):
        """
        Iterates this object for its values.  This will return the field names from the
        database rather than the API names.  If you want the API names, you should use
        the recordValues method.

        :sa         recordValues

        :return     <iter>
        """
        return self.json()

    def __iter__(self):
        """
        Iterates this object for its values.  This will return the field names from the
        database rather than the API names.  If you want the API names, you should use
        the recordValues method.

        :sa         recordValues

        :return     <iter>
        """
        data = self.recordValues(key='field', inflated=False)
        for key, value in data.items():
            yield key, value

    def __format__(self, format_spec):
        """
        Formats this record based on the inputted format_spec.  If no spec
        is supplied, this is the same as calling str(record).
        
        :param      format_spec | <str>
        
        :return     <str>
        """
        if not format_spec:
            return nstr(self)
        if format_spec == 'primaryKey':
            return nstr(self.primaryKey())
        else:
            column = self.schema().column(format_spec)
            if column:
                return nstr(self.recordValue(format_spec))

        return super(Table, self).__format__(format_spec)

    def __str__(self):
        """
        Defines the custom string format for this table.
        """
        return projex.text.toBytes(unicode(self))

    def __unicode__(self):
        """
        Defines the custom string format for this table.
        """
        schema = self.schema()
        sform = None

        # extract any inherited 
        while schema:
            sform = schema.stringFormat()
            if sform:
                break
            else:
                schema = orb.system.schema(schema.inherits())

        if not sform:
            return unicode(super(Table, self).__str__())
        else:
            return unicode(sform).format(self, self=self)

    def __eq__(self, other):
        """
        Checks to see if the two records are equal to each other
        by comparing their primary key information.
        
        :param      other       <variant>
        
        :return     <bool>
        """
        if id(self) == id(other):
            return True
        elif isinstance(other, Table) and hash(self) == hash(other):
            return True
        else:
            return False

    def __ne__(self, other):
        """
        Returns whether or not this object is not equal to the other object.
        
        :param      other | <variant>
        
        :return     <bool>
        """
        return not self.__eq__(other)

    def __hash__(self):
        """
        Creates a hash key for this instance based on its primary key info.
        
        :return     <int>
        """
        # use the base id information
        if not self.isRecord():
            return super(Table, self).__hash__()

        # return a combination of its table and its primary key hashes
        return hash((self.__class__, self.database(), self.primaryKey()))

    def __cmp__(self, other):
        """
        Compares one record to another.
        
        :param      other | <variant>
        
        :return     -1 || 0 || 1
        """
        return cmp(nstr(self), nstr(other))

    def __init__(self, *args, **kwds):
        """
        Initializes a database record for the table class.  A
        table model can be initialized in a few ways.  Passing
        no arguments will create a fresh record that does not
        exist in the database.  Providing keyword arguments will
        map to this table's schema column name information, 
        setting default values for the record.  Supplying an
        argument will be the records unique primary key, and 
        trigger a lookup from the database for the record directly.
        
        :param      *args       <tuple> primary key
        :param      **kwds      <dict>  column default values
        """
        # define table properties in a way that shouldn't be accidentally
        # overwritten
        self.__local_cache_lock = ReadWriteLock()
        self.__local_cache = {}
        self.__record_defaults = {}
        self.__record_value_lock = ReadWriteLock()
        self.__record_values = {}
        self.__record_dbloaded = set()
        self.__record_datacache = None

        self.__lookup_options = orb.LookupOptions(**kwds)
        self.__context_options = orb.ContextOptions(**kwds)

        # initialize the defaults
        if len(args) == 1 and type(args[0]) == dict and args[0].get('__pickle'):
            self.__setstate__(args[0].pop('__pickle'))

        elif '__values' in kwds:
            self._updateFromDatabase(kwds.pop('__values'))

        elif not args:
            self.initRecord()
            self.resetRecord()

        # initialize from the database
        else:
            # extract the primary key for initializing from a record
            if len(args) == 1 and (Table.recordcheck(args[0]) or orb.View.recordcheck(args[0])):
                record = args[0]
                args = record.id()
                self._updateFromDatabase(dict(record))

            elif len(args) == 1:
                args = args[0]

            cache = self.tableCache()
            cache_key = '__init__({0})'.format(args)
            try:
                data = cache[cache_key]
            except (TypeError, KeyError):
                data = self.getRecord(args, inflated=False, options=self.__context_options)
                if data is not None and cache:
                    cache[cache_key] = data

            if data:
                self._updateFromDatabase(data)
            else:
                raise errors.RecordNotFound(self, args)

        self.setRecordValues(**{k: v for k, v in kwds.items() if self.schema().column(k)})

    #----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    def _markAsLoaded(self, database=None, columns=None):
        """
        Goes through and marks all the columns as loaded from the database.
        
        :param      columns | [<str>, ..] || None
        """
        columns = [self.schema().column(column) for column in columns] if columns else self.schema().columns()

        with WriteLocker(self.__record_value_lock):
            for column in columns:
                data = self.__record_values.get(column)
                if type(data) == dict:
                    data = data.copy()
                self.__record_defaults[column] = data

            self.__context_options.database = database
            self.__record_dbloaded.update(columns)

    def _updateFromDatabase(self, data):
        """
        Called from the backend class when it needs to
        manipulate information on this record instance.
        
        :param      data    | {<str> column_name: <variant>, ..}
                    options | <orb.ContextOptions>
        """
        lookup = self.lookupOptions()
        options = self.contextOptions()
        schema = self.schema()
        dbname = schema.dbname()
        dvalues = {}
        loaders = []

        for column_name, value in data.items():
            try:
                tname, colname = column_name.split('.')
            except ValueError:
                colname = column_name
                tname = dbname

            # make sure this value is from the database
            if tname != dbname:
                continue

            # retrieve the column information
            column = schema.column(colname)

            # use the expanded option when possible
            if column in dvalues and (orb.Table.recordcheck(dvalues[column]) or orb.View.recordcheck(dvalues[column])):
                continue

            if not column:
                # preload records for reverse lookups and pipes
                try:
                    loader = getattr(self, colname).preload
                    loaders.append((loader, value))
                except AttributeError:
                    pass
                continue

            # preload a reference column
            elif column.isReference() and (type(value) == dict or
                                           (type(value) in (str, unicode) and value.startswith('{'))):
                if type(value) in (str, unicode) and value.startswith('{'):
                    try:
                        value = eval(value)
                    except StandardError:
                        raise errors.OrbError('Invalid reference found: {0}'.format(value))

                model = column.referenceModel()
                if not model:
                    raise errors.TableNotFound(column.reference())
                value = model(__values=value, options=options)

            # restore translatable columns
            elif column.isTranslatable():
                if type(value) in (str, unicode) and value.startswith('{'):
                    try:
                        value = eval(value)
                    except StandardError:
                        value = None
                elif options and options.locale != 'all':
                    value = {options.locale: value}
                else:
                    value = {self.recordLocale(): value}

            # map a query value to a query
            elif column.columnType() == ColumnType.Query:
                if type(value) == dict:
                    value = orb.Query.fromDict(value)
                elif type(value) in (str, unicode):
                    value = orb.Query.fromXmlString(value)

            dvalues[column] = value
            self.__record_dbloaded.add(column)

        # set data properties
        with WriteLocker(self.__record_value_lock):
            column_values = {k: v if type(v) != dict else v.copy() for k, v in dvalues.items()}
            self.__record_values.update(column_values)
            self.__record_defaults.update({k: v if type(v) != dict else v.copy() for k, v in dvalues.items()})

        # load pre-loaded information from database
        for loader, value in loaders:
            for preload_type, preload_value in value.items():
                loader(self, preload_value, options, type=preload_type)

    def _removedFromDatabase(self):
        """
        Called after a record has been removed from the
        database, so the record instance can clean up
        any additional information.
        """
        self.__record_defaults.clear()
        self.__record_dbloaded.clear()

    #----------------------------------------------------------------------
    #                       PRIVATE METHODS
    #----------------------------------------------------------------------
    def changeset(self, columns=None, recurse=True, flags=0, kind=0, inflated=False):
        """
        Returns a dictionary of changes that have been made
        to the data from this record.
        
        :return     { <orb.Column>: ( <variant> old, <variant> new), .. }
        """
        changes = {}
        is_record = self.isRecord()
        kind = kind or orb.Column.Kind.Field
        columns = columns or self.schema().columns(recurse=recurse, flags=flags, kind=kind)

        with ReadLocker(self.__record_value_lock):
            for column in columns:
                newValue = self.__record_values.get(column)

                # assume all changes for a new record
                if not is_record:
                    oldValue = None

                    # ignore read only columns for initial insert
                    if column.isReadOnly():
                        continue

                # only look for changes from loaded columns
                elif column in self.__record_dbloaded:
                    oldValue = self.__record_defaults.get(column)

                # otherwise, ignore the change
                else:
                    continue

                # compare two queries
                if orb.Query.typecheck(newValue) or orb.Query.typecheck(oldValue):
                    equals = hash(oldValue) == hash(newValue)

                # compare two datetimes
                elif isinstance(newValue, datetime.datetime) and \
                        isinstance(oldValue, datetime.datetime):
                    try:
                        equals = newValue == oldValue

                    # compare against non timezone values
                    except TypeError:
                        norm_new = newValue.replace(tzinfo=None)
                        norm_old = oldValue.replace(tzinfo=None)
                        equals = norm_new == norm_old

                # compare a table against a non-table
                elif Table.recordcheck(newValue) or Table.recordcheck(oldValue):
                    if isinstance(newValue, (int, long)):
                        equals = oldValue.primaryKey() == newValue
                    elif isinstance(oldValue, (int, long)):
                        equals = newValue.primaryKey() == oldValue
                    else:
                        equals = newValue == oldValue

                # compare all other types
                else:
                    try:
                        equals = newValue == oldValue
                    except UnicodeWarning:
                        equals = False

                if not equals:
                    if inflated and column.isReference():
                        model = column.referenceModel()
                        if not isinstance(oldValue, model):
                            oldValue = model(oldValue) if oldValue else None
                        if not isinstance(newValue, model):
                            newValue = model(newValue) if newValue else None
                    changes[column] = (oldValue, newValue)

        return changes

    def clearCustomCache(self):
        """
        Clears out any custom cached data.  This is a pure virtual method,
        as by default the Table class does not define any direct custom
        cache information.  Overload this method to wipe any local data that
        is cached when the system decides to clear.
        """
        pass

    def commit(self, *args, **kwds):
        """
        Commits the current change set information to the database,
        or inserts this object as a new record into the database.
        This method will only update the database if the record
        has any local changes to it, otherwise, no commit will
        take place.  If the dryRun flag is set, then the SQL
        will be logged but not executed.
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, 'options' for
                    an instance of the <orb.ContextOptions>
        
        :return     <bool> success
        """

        # sync the record to the database
        lookup = orb.LookupOptions(**kwds)
        options = orb.ContextOptions(**kwds)

        # run any pre-commit logic required for this record
        self.callbacks().emit('aboutToCommit(Record,LookupOptions,ContextOptions)', self, lookup, options)

        # check to see if we have any modifications to store
        if not self.isModified():
            return False

        # validate the data from the software side that is about to be saved
        self.validateRecord()

        # support columns as defined by a variable list of arguments
        if args:
            columns = set(kwds.get('columns', []))
            columns.update(args)
            kwds['columns'] = list(columns)

        # grab the database
        db = kwds.pop('db', None)
        if db is None:
            db = self.database()

        if db is None:
            raise errors.DatabaseNotFound()

        # grab the backend
        backend = db.backend()
        results = backend.storeRecord(self, lookup, options)

        # marks this table as expired
        self.markTableCacheExpired()

        # store the archive
        self.commitToArchive(*args, **kwds)

        # clear any custom caches
        with WriteLocker(self.__local_cache_lock):
            self.__local_cache.clear()
            self.__context_options.database = db

        self.clearCustomCache()

        # run any pre-commit logic required for this record
        kwds['results'] = results

        cache = self.tableCache()
        if cache:
            cache.expire(self.primaryKey())

        self.callbacks().emit('commitFinished(Record,LookupOptions,ContextOptions)', self, lookup, options)
        return True

    def commitToArchive(self, *args, **kwds):
        """
        Saves this record to an archive location.

        :return     <bool> | success
        """
        if not self.schema().isArchived():
            return False

        if not self.isRecord():
            raise errors.RecordNotFound(type(self), self.id())

        model = self.schema().archiveModel()
        if not model:
            raise errors.ArchiveNotFound(self.schema().name())

        try:
            last_archive = self.archives().last()
        except AttributeError:
            raise errors.ArchiveNotFound(self.schema().name())

        number = last_archive.archiveNumber() if last_archive else 0

        # create the new archive information
        values = self.recordValues(inflated=False, flags=~orb.Column.Flags.Primary)
        locale = values.get('locale') or kwds.get('locale') or self.recordLocale()
        record = model(**values)
        record.setArchivedAt(datetime.datetime.now())
        record.setArchiveNumber(number + 1)
        record.setRecordValue(projex.text.camelHump(self.schema().name()), self)

        try:
            record.setLocale(locale)  # property on archive for translatable models
        except AttributeError:
            pass

        record.commit()
        return True

    def conflicts(self, *columnNames, **options):
        """
        Looks up conflicts from the database by comparing values for specific
        (or all) columns from the database against the local default and 
        value list.  The returned value will be a dictionary containing the
        conflicted column as the key, and a tuple containing the database
        value and the local value cache.  This method is useful to validate
        data being committed to the database won't conflict with what was
        stored when the record was generated.
        
        :sa         reload
        
        :param      columnNames | <varg> [<str> columnName, ..]
        
        :return     {<orb.Column>: (<var> db value, <var> local value, ..)
        """
        inflated = options.pop('inflated', False)
        if not self.isRecord():
            return {}

        schema = self.schema()
        if not columnNames:
            columnNames = schema.columnNames()

        query = Q(type(self)) == self
        values = self.selectFirst(columns=columnNames,
                                  where=query,
                                  inflated=False)

        # look for clashing changes
        conflicts = {}
        with ReadLocker(self.__record_value_lock):
            for colname, d_value in values.items():
                column = self.schema().column(colname)

                # don't care about non-loaded columns
                if not (column and column in self.__record_dbloaded):
                    continue

                m_default = self.__record_defaults[column]
                m_value = self.__record_values[column]

                if inflated:
                    ref_model = column.referenceModel()
                    if ref_model:
                        if m_default is not None:
                            m_default = ref_model(m_default)
                        if m_value is not None:
                            m_value = ref_model(m_value)
                        if d_value is not None:
                            d_value = ref_model(d_value)
                else:
                    # always do a primary key comparison since we won't be inflated
                    # values from the database
                    if Table.recordcheck(m_value):
                        m_value = m_value.primaryKey()
                    if Table.recordcheck(m_default):
                        m_default = m_default.primaryKey()

                # ignore unchanged values, we can update without issue
                if m_value == m_default:
                    continue

                # ignore unchanged values from the database, we can save without
                # conflict
                elif d_value in (m_default, m_value):
                    continue

                # otherwise, mark the conflict
                conflicts[column] = (d_value, m_value)

        return conflicts

    def database(self):
        """
        Returns the database instance for this record.  If no
        specific database is defined, then the database will be looked up
        based on the name, environment, and current settings from the current
        manager.
        
        :return     <Database> || None
        """
        return self.__context_options.raw_values.get('database') or self.getDatabase()

    def dataCache(self):
        """
        Returns the cache instance record for this record.
        
        :return     <orb.DataCache>
        """
        if not self.__record_datacache:
            self.__record_datacache = orb.DataCache.create()
        return self.__record_datacache

    def contextOptions(self, **options):
        """
        Returns the lookup options for this record.  This will track the options that were
        used when looking this record up from the database.

        :return     <orb.LookupOptions>
        """
        output = self.__context_options.copy()
        output.update(options)
        return output

    def duplicate(self):
        """
        Creates a new record based on this instance, initializing
        it with the data from this record.
        
        :return     <Table>
        """
        pcols = self.schema().primaryColumns()
        with ReadLocker(self.__record_value_lock):
            db_values = {col: value for col, value in self.__record_values.items() if col not in pcols}

        inst = self.__class__()
        with WriteLocker(inst._Table__record_values):
            inst._Table__record_values = db_values
        return inst

    def findAllRelatedRecords(self):
        """
        Looks up all related records to this record via all the relations
        within the database that would point back to this instance.
        
        :return     {(<orb.Table>, <orb.Column>): <orb.RecordSet>), ..}
        """
        # lookup cascading removal
        relations = orb.system.findRelations(self.schema())
        output = {}
        for table, columns in relations:
            for column in columns:
                q = Q(column.name()) == self
                output[(table, column)] = table.select(where=q)

        return output

    def initRecord(self):
        """
        Initializes the default values for this record.
        """
        for column in self.schema().columns(kind=orb.Column.Kind.Field):
            value = column.default(resolve=True)
            if column.isTranslatable():
                value = {self.recordLocale(): value}
            elif column.isString() and column.testFlag(column.Flags.Polymorphic):
                value = type(self).__name__

            self.__record_defaults[column] = value

    def insertInto(self, db, **options):
        """
        Inserts this record into another database.  This method will allow
        for easy duplication of one record in one database to another database.
        
        :param      db | <orb.Database>
        """
        if self.database() == db:
            return False

        lookup = orb.LookupOptions(**options)
        ctxt_opts = orb.ContextOptions(**options)
        ctxt_opts.force = True

        backend = db.backend()
        try:
            backend.insert([self], lookup, ctxt_opts)
            return True
        except errors.OrbError, err:
            if ctxt_opts.throwErrors:
                raise
            else:
                log.error('Backend error occurred.\n%s', err)
                return False

    def isModified(self):
        """
        Returns whether or not any data has been modified for
        this object.
        
        :return     <bool>
        """
        if not self.isRecord():
            return True
        return len(self.changeset()) > 0

    def isRecord(self, db=None):
        """
        Returns whether or not this database table record exists
        in the database.
        
        :return     <bool>
        """
        if db in (None, self.database()):
            # make sure we have an ID and that the ID has been loaded from the database
            primary_cols = self.schema().primaryColumns()
            if not primary_cols:
                raise orb.errors.PrimaryKeyNotDefined(self)
            return bool(self.primaryKey()) and self.__record_dbloaded.issuperset(primary_cols)
        return False

    def json(self, **options):
        """
        Converts this object to a JSON string or dictionary.

        :return     <dict> || <str>
        """
        # additional options
        lookup = self.lookupOptions(**options)
        ctxt_opts = self.contextOptions(**options)

        # hide private columns
        schema = self.schema()
        columns = [schema.column(x) for x in lookup.columns] if lookup.columns else schema.columns()
        columns = [x for x in columns if x and not x.testFlag(x.Flags.Private)]

        # simple json conversion
        output = self.recordValues(key='field', columns=columns, inflated=False)

        # expand any references we need
        expand_tree = lookup.expandtree()
        if expand_tree:
            for key, subtree in expand_tree.items():
                try:
                    getter = getattr(self, key)
                except AttributeError:
                    continue
                else:
                    value = getter(options=ctxt_opts)
                    try:
                        updater = value.updateOptions
                    except AttributeError:
                        pass
                    else:
                        updater(expand=subtree, returning=lookup.returning)
                    output[key] = value

        # don't include the column names
        if lookup.returning == 'values':
            output = [output[column.fieldName()] for column in lookup.schemaColumns(self.schema())]
            if len(output) == 1:
                output = output[0]

        if ctxt_opts.format == 'text':
            return projex.rest.jsonify(output)
        else:
            return output

    def localCache(self, key, default=None):
        """
        Returns the data from the local cache for the given key.
        
        :param      key     | <str>
                    default | <variant>
        """
        with ReadLocker(self.__local_cache_lock):
            return self.__local_cache.get(key, default)

    def lookupOptions(self, **options):
        """
        Returns the lookup options for this record.  This will track the options that were
        used when looking this record up from the database.

        :return     <orb.LookupOptions>
        """
        output = self.__lookup_options or orb.LookupOptions()
        output.update(options)
        return output

    def primaryKey(self):
        """
        Returns the values for the primary key for this record.
        It is important to note, that this will return the column
        values as they are in the database, not as they are on the
        class instance.
        
        :return     <variant> | will return a tuple or column value
        
        :usage      |>>> # this is just an example, not a provided class
                    |>>> from somemodule import Person
                    |>>> p = Person('Eric','Hulser')
                    |>>> p.isRecord()
                    |True
                    |>>> [ col.name() for col in p.schema().primaryColumns() ]
                    |['firstName','lastName']
                    |>>> p.primaryKey()
                    |('Eric','Hulser')
                    |>>> p.setLastName('Smith')
                    |>>> # accessing the column reflects the data on the object
                    |>>> p.firstName(), p.lastName()
                    |('Eric','Smith')
                    |>>> # accessing the pkey reflects the data in the db
                    |>>> p.primaryKey()
                    |('Eric','Hulser')
                    |>>> # committing to the db will update the database,
                    |>>> # and on success, update the object record
                    |>>> p.commit()
                    |{'updated',{'lastName': ('Hulser','Smith')})
                    |>>> # now that the changes are in the DB, the pkey is
                    |>>> # updated to reflect the record
                    |>>> p.primaryKey()
                    |('Eric','Smith')
        """
        cols = self.schema().primaryColumns()
        defaults = self.__record_defaults
        output = []
        for col in cols:
            if col not in self.__record_dbloaded:
                return None
            output.append(defaults.get(col))

        if len(output) == 1:
            return output[0]
        return tuple(output)

    def primaryKeyTuple(self):
        """
        Returns this records primary key as a tuple.
        
        :return     (<variant>, ..)
        """
        cols = self.schema().primaryColumns()
        return tuple([self.__record_defaults.get(col) for col in cols])

    def primaryKeyDict(self):
        """
        Returns a dictionary of the primary key information for this
        record.
        
        :return     <dict>
        """
        cols = self.schema().primaryColumns()
        return {col: self.__record_defaults.get(col) for col in cols}

    def recordNamespace(self):
        """
        Returns the records specific namespace.  This can be used to override
        particular settings for a record.
        
        :return     <str>
        """
        return self.__context_options.namespace

    def recordLocale(self):
        """
        Returns the locale that this record represents, if no locale has been defined, then the global
        orb system locale will be used.

        :return     <str>
        """
        return self.contextOptions().locale

    def recordValue(self,
                    column,
                    locale=None,
                    default=None,
                    inflated=True,
                    useMethod=True):
        """
        Returns the value for the column for this record.
        
        :param      column      | <orb.Column> || <str>
                    default     | <variant>
                    inflated    | <bool>
        
        :return     <variant>
        """
        col = self.schema().column(column)
        if not col:
            raise errors.ColumnNotFound(self.schema().name(), column)

        if useMethod:
            method = getattr(self.__class__, col.getterName(), None)
            try:
                orb_getter = type(method.im_func).__name__ == 'gettermethod'
            except AttributeError:
                orb_getter = False

            if method is not None and not orb_getter:
                keywords = list(self.__getKeywords(method))
                kwds = {}

                if 'locale' in keywords:
                    kwds['locale'] = locale
                if 'default' in keywords:
                    kwds['default'] = default
                if 'inflated' in keywords:
                    kwds['inflated'] = inflated

                return method(self, **kwds)

        try:
            with ReadLocker(self.__record_value_lock):
                value = self.__record_values[col]
        except KeyError:
            proxy = self.proxyColumn(column)
            if proxy and proxy.getter():
                return proxy.getter()(self)
            return default

        # return the translatable value
        if col.isTranslatable():
            if value is None:
                return ''

            # return all the locales
            if locale == 'all':
                return value

            # return specific locales
            elif type(locale) in (list, tuple, set):
                output = {}
                for lang in locale:
                    output[lang] = value.get(lang)
                return output

            # return a set of locales
            elif type(locale) == dict:
                # return first in the set
                if 'first' in locale:
                    langs = set(value.keys())
                    first = list(locale['first'])
                    remain = list(langs.difference(first))

                    for lang in first + remain:
                        val = value.get(lang)
                        if val:
                            return val
                return ''

            # return the current locale
            elif locale is None:
                locale = self.recordLocale()

            value = value.get(locale)
            if not value:
                value = default

        # return a reference when desired
        if col.isReference() and inflated and value is not None:
            # ensure we have a proper reference model
            refmodel = col.referenceModel()
            if refmodel is None:
                raise errors.TableNotFound(col.reference())

            # make sure our value already meets the criteria
            elif refmodel.recordcheck(value):
                return value

            # inflate the value to the class value
            inst = refmodel(value, db=self.database(), options=self.contextOptions())
            if value == self.__record_defaults.get(col):
                self.__record_defaults[col] = inst

            # cache the record value
            with WriteLocker(self.__record_value_lock):
                self.__record_values[col] = inst
            return inst

        else:
            options = self.contextOptions()
            return col.restoreValue(value, options) if not Table.recordcheck(value) else value.id()

    def recordValues(self,
                     columns=None,
                     inflated=False,
                     recurse=True,
                     flags=0,
                     kind=0,
                     mapper=None,
                     locale=None,
                     key='name'):
        """
        Returns a dictionary grouping the columns and their
        current values.  If the inflated value is set to True,
        then you will receive any foreign keys as inflated classes (if you
        have any values that are already inflated in your class, then you
        will still get back the class and not the primary key value).  Setting
        the mapper option will map the value by calling the mapper method.
        
        :param      useFieldNames | <bool>
                    inflated | <bool>
                    mapper | <callable> || None
        
        :return     { <str> key: <variant>, .. }
        """
        output = {}
        schema = self.schema()
        all_columns = schema.columns(recurse=recurse, flags=flags, kind=kind)
        req_columns = [schema.column(col) for col in columns] if columns else None

        for column in all_columns:
            if req_columns is not None and column not in req_columns:
                continue

            if key == 'name':
                val_key = column.name()
            elif key == 'field':
                val_key = column.fieldName()
            elif key == 'column':
                val_key = column
            else:
                raise errors.OrbError('Invalid key request.')

            value = self.recordValue(column, inflated=inflated, locale=locale)
            if mapper:
                value = mapper(value)

            output[val_key] = value

        return output

    def recordLocales(self):
        """
        Collects a list of all the locales associated with this record.

        :return     {<str>, ..}
        """
        translatable = [col for col in self.schema().columns() if col.isTranslatable()]
        output = set()
        with ReadLocker(self.__record_value_lock):
            values = self.__record_values.items()

        for column, value in values:
            if column in translatable and type(value) == dict:
                output.update(value.keys())
        return output

    def reload(self, *columnNames, **kwds):
        """
        Reloads specific columns from the database for this record.  This will
        replace both the default and value for this table, so any local cache
        will be replaced.  The returned dictionary will contain a set of
        conflicts from the database.  The key will be the column that is
        conflicting, and the value will be a tuple containing the database
        value and the local value.
        
        :param      columnNames | <varg> (<str> columnName, ..)
                    options     | <Table.ReloadOptions>
        
        :return     {<orb.Column>: (<var> db value, <var> local value, ..)
        """
        opts = Table.ReloadOptions
        reload_options = kwds.get('options',
                                  Table.ReloadOptions.IgnoreConflicts)

        if not self.isRecord():
            return {}

        if not columnNames:
            columns = self.schema().columns()
        else:
            columns = [self.schema().column(colname) for colname in columnNames]

        # only update unmodified columns
        if reload_options & (opts.Modified | opts.Unmodified):
            with WriteLocker(self.__record_value_lock):
                for column in self.__record_values:
                    m_value = self.__record_values[column]
                    m_default = self.__record_defaults[column]

                    if reload_options & opts.Unmodified and m_value != m_default:
                        try:
                            columns.remove(column)
                        except ValueError:
                            continue

                    elif reload_options & opts.Modified and m_value == m_default:
                        try:
                            columns.remove(column)
                        except ValueError:
                            continue

        # don't look anything up if there are no values
        if not columns:
            return {}

        query = Q(type(self)) == self
        values = self.selectFirst(columns=columns,
                                  where=query,
                                  inflated=False)

        # look for clashing changes
        conflicts = {}
        updates = {}
        with WriteLocker(self.__record_value_lock):
            for colname, d_value in values.items():
                column = self.schema().column(colname)
                if not reload_options & opts.IgnoreConflicts:
                    updates[column] = d_value

                # don't care about non-loaded columns
                if column not in self.__record_dbloaded:
                    continue

                m_default = self.__record_defaults[column]
                m_value = self.__record_values[column]

                # ignore unchanged values, we can update without issue
                if m_value == m_default:
                    continue

                # ignore unchanged values from the database, we can save without
                # conflict
                elif d_value == m_default:
                    continue

                # otherwise, mark the conflict
                conflicts[column] = (d_value, m_value)

            # update the record internals
            self.__record_dbloaded.update(values.keys())
            self.__record_defaults.update(values)
            self.__record_values.update(values)

        return conflicts

    def remove(self, **kwds):
        """
        Removes this record from the database.  If the dryRun \
        flag is specified then the command will be logged and \
        not executed.
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'options' for 
                    an instance of the <orb.ContextOptions>
        
        :return     <int>
        """
        if not self.isRecord():
            return 0

        cls = type(self)
        opts = orb.ContextOptions(**kwds)
        lookup = orb.LookupOptions(**kwds)
        lookup.where = Q(cls) == self

        try:
            return self.database().backend().remove(cls, lookup, opts)
        except AttributeError:
            return 0

    def resetRecord(self):
        """
        Resets the values for this record to the database
        defaults.  This will only reset to the local cache, not reload from
        the database itself.  To reset the record from database values, 
        use the reload method.
        
        :sa     reload
        """
        with WriteLocker(self.__record_value_lock):
            values = {k: v if type(v) != dict else v.copy() for k, v in self.__record_defaults.items()}
            self.__record_values = values

    def revert(self, *columnNames, **kwds):
        """
        Reverts all conflicting column data from the database so that the
        local modifications are up to date with what is in the database.

        :sa         reload

        :param      columnNames | <varg> [<str> columnName, ..]
                    options     | <Table.ReloadOptions>
        """
        kwds.setdefault('options', Table.ReloadOptions.Conflicts)
        self.reload(*columnNames, **kwds)

    def setDatabase(self, database):
        """
        Sets the specific database instance that this
        record will be using when executing transactions.
        
        :param      database        <Database> || None
        """
        self.__context_options.database = database

    def setContextOptions(self, options):
        self.__context_options = options

    def setLocalCache(self, key, value):
        """
        Sets a value for the local cache to the inputted key & value.
        
        :param      key     | <str>
                    value   | <variant>
        """
        with WriteLocker(self.__local_cache_lock):
            self.__local_cache[key] = value

    def setLookupOptions(self, lookup):
        self.__lookup_options = lookup

    def setRecordDefault(self, columnName, value, locale=None):
        """
        Sets the default value for the column name at the given value.
        
        :param      columnName | <str>
                    value      | <variant>
        
        :return     <bool> | success
        """
        column = self.schema().column(columnName)

        # check to see if we're setting a proxy column
        if not column:
            proxy = self.proxyColumn(columnName)
            if proxy and proxy.setter():
                proxy.setter()(self, value)
                return True

            raise errors.ColumnNotFound(self.schema().name(), columnName)

        # otherwise, store the column information in the defaults
        value = column.storeValue(value)
        locale = locale or self.recordLocale()

        with WriteLocker(self.__record_value_lock):
            if column.isTranslatable():
                self.__record_defaults.setdefault(column, {})
                self.__record_values.setdefault(column, {})

                self.__record_defaults[column][locale] = value
                self.__record_values[column][locale] = value
            else:
                self.__record_defaults[column] = value
                self.__record_values[column] = value

        return True

    def setRecordLocale(self, locale):
        """
        Sets the default locale for this record to the inputted value.  This will affect what locale
        is stored when editing and what is returned on reading.  If no locale is supplied, then
        the default system locale will be used.

        :param      locale | <str> || None
        """
        self.__context_options.locale = locale

    def setRecordValue(self,
                       columnName,
                       value,
                       useMethod=True,
                       locale=None):
        """
        Sets the value for this record at the inputted column
        name.  If the columnName provided doesn't exist within
        the schema, then the ColumnNotFound error will be
        raised.
        
        :param      columnName      | <str>
                    value           | <variant>
        
        :return     <bool> changed
        """
        # convert the inputted value information
        value = orb.DataConverter.toPython(value)

        # validate the column
        column = self.schema().column(columnName)
        if not column:
            raise errors.ColumnNotFound(self.schema().name(), columnName)

        # set a proxy value
        proxy = self.proxyColumn(columnName)
        if proxy and proxy.setter():
            result = proxy.setter()(self, value)
            if result is not None:
                return result
            return True

        elif proxy:
            raise errors.ColumnReadOnly(column)

        elif useMethod:
            method = getattr(self.__class__, column.setterName(), None)
            try:
                orb_setter = type(method.im_func).__name__ == 'settermethod'
            except AttributeError:
                orb_setter = False

            if method is not None and not orb_setter:
                keywords = list(self.__getKeywords(method))
                if 'locale' in keywords:
                    return method(self, value, locale=locale)
                else:
                    return method(self, value)

        # cannot update aggregate or join columns
        if column.isReadOnly():
            raise errors.ColumnReadOnly(column)

        # make sure the inputted value matches the validation
        column.validate(value)
        equals = False

        # store the new value
        with WriteLocker(self.__record_value_lock):
            curr_value = self.__record_values.get(column)

            if column.isTranslatable():
                if curr_value is None:
                    curr_value = {self.recordLocale(): ''}
                    self.__record_values[column] = curr_value

                if type(value) == dict:
                    equals = False
                    curr_value.update(value)
                else:
                    value = column.storeValue(value)
                    locale = locale or self.recordLocale()

                    try:
                        equals = curr_value[locale] == value
                    except (KeyError, UnicodeWarning):
                        equals = False

                    curr_value[locale] = value
            else:
                value = column.storeValue(value)

                # test comparison for queries
                if orb.Query.typecheck(value) or orb.Query.typecheck(curr_value):
                    equals = hash(value) == hash(curr_value)
                else:
                    try:
                        equals = curr_value == value
                    except TypeError:
                        # compare timezone agnostic values
                        if isinstance(curr_value, datetime.datetime) and isinstance(value, datetime.datetime):
                            equals = orb.system.asutc(curr_value) == orb.system.asutc(value)
                    except UnicodeWarning:
                        equals = False

                self.__record_values[column] = value
        return not equals

    def setRecordValues(self, **data):
        """
        Sets the values for this record from the inputted column
        value pairing
        
        :param      **data      key/value pair for column names
        
        :return     <int> number set
        """
        for colname, value in data.items():
            self.setRecordValue(colname, value)
        return len(data)

    def setRecordNamespace(self, namespace):
        """
        Sets the namespace that will be used by this record in the database.
        If no namespace is defined, then it will inherit from its table settings.
        
        :param      namespace | <str> || None
        """
        self.__context_options.namespace = namespace

    def update(self, **values):
        return self.setRecordValues(**values)

    def updateOptions(self, **options):
        self.__lookup_options.update(options)
        self.__context_options.update(options)

    def updateFromRecord(self, record):
        """
        Updates this records values from the inputted record.
        
        :param      record | <orb.Table>
        """
        changes = record.changeset()
        for column, values in changes.items():
            try:
                self.setRecordValue(column, values[1])
            except errors.OrbError:
                pass

    def validateRecord(self, overrides=None):
        """
        Validates the current record object to make sure it is ok to commit to the database.  If
        the optional override dictionary is passed in, then it will use the given values vs. the one
        stored with this record object which can be useful to check to see if the record will be valid before
        it is committed.

        :param      overrides | <dict>

        :return     <bool>
        """
        schema = self.schema()
        with ReadLocker(self.__record_value_lock):
            values = {k: v if type(v) != dict else v.copy() for k, v in self.__record_values.items()}
            overrides = overrides or {}
            values.update({schema.column(col): v for col, v in overrides.items() if schema.column(col)})

        # validate the columns
        for column, value in values.items():
            column.validate(value)

        # validate the indexes
        for index in self.schema().indexes():
            index.validate(self, values)

        # validate the record against all custom validators
        for validator in self.schema().validators():
            validator.validate(self, values)

        return True

    #----------------------------------------------------------------------
    #                           CLASS METHODS
    #----------------------------------------------------------------------

    @classmethod
    def all(cls, **options):
        """
        Returns a record set containing all records for this table class.  This
        is a convenience method to the <orb.Table>.select method.
        
        :param      **options | <orb.LookupOptions> & <orb.ContextOptions>
        
        :return     <orb.RecordSet>
        """
        return cls.select(**options)

    @classmethod
    def accessibleQuery(cls, **options):
        """
        Returns the query to be used to filter accessible models from the database.  This method should be
        overridden to match your specific authorization system, but can be used to hide particular records
        from the database.

        :return     <orb.Query> || None
        """
        return None

    @classmethod
    def createRecord(cls, values, **options):
        """
        Shortcut for creating a new record for this table.

        :param     values | <dict>

        :return    <orb.Table>
        """
        # extract additional options for return
        expand = [item for item in values.pop('expand', '').split(',') if item]

        # create the new record
        record = cls(options=orb.ContextOptions(**options))
        record.update(**values)
        record.commit()

        if expand:
            lookup = record.lookupOptions()
            lookup.expand = expand
            record.setLookupOptions(lookup)

        return record

    @classmethod
    def currentRecord(cls):
        """
        Returns the current record that was tagged for usage.
        
        :return     <orb.Table> || None
        """
        return getattr(cls, '_{0}__current'.format(cls.__name__), None)

    @classmethod
    def defineProxy(cls, typ, getter, setter=None, **options):
        """
        Defines a new proxy column.  Proxy columns are code based properties -
        the information will be generated by methods and not stored directly in
        the database, however can be referenced abstractly as though they are
        columns.  This is useful for generating things like joined or calculated
        column information.
        
        :param      columnName | <str>
                    getter     | <callable>
                    setter     | <callable>
        """
        proxies = getattr(cls, '_%s__proxies' % cls.__name__, {})

        name = options.get('name', getter.__name__)

        options['getter'] = getter
        options['setter'] = setter
        options['proxy'] = True
        options['schema'] = cls.schema()

        col = orb.Column(typ, name, **options)

        proxies[name] = col
        setattr(cls, '_%s__proxies' % cls.__name__, proxies)

    @classmethod
    def defineRecord(cls, **kwds):
        """
        Defines a new record for the given class based on the
        inputted set of keywords.  If a record already exists for
        the query, the first found record is returned, otherwise
        a new record is created and returned.
        
        :param      **kwds | columns & values
        """
        # require at least some arguments to be set
        if not kwds:
            return cls()

        # lookup the record from the database
        db = kwds.pop('db', None)
        q = Q()

        for key, value in kwds.items():
            column = cls.schema().column(key)
            if not column:
                raise orb.errors.ColumnNotFound(cls.schema().name(), key)

            if column.isString() and \
                    not column.testFlag(column.Flags.CaseSensitive) and \
                    isinstance(value, (str, unicode)):
                q &= Q(column.name()).lower() == value.lower()
            else:
                q &= Q(key) == value

        record = cls.select(where=q, db=db).first()
        if not record:
            record = cls(**kwds)
            record.commit(db=db)

        return record

    @classmethod
    def baseQuery(cls, **options):
        """
        Returns the default query value for the inputted class.  The default
        table query can be used to globally control queries run through a 
        Table's API to always contain a default.  Common cases are when
        filtering out inactive results or user based results.
        
        :return     <orb.Query> || None
        """
        return getattr(cls, '_%s__baseQuery' % cls.__name__, None)

    @classmethod
    def generateToken(cls, column, prefix='', suffix=''):
        """
        Generates a new random token for the token record based off the
        database.
        
        :param      column | <str>
                    prefix | <str>
                    suffix | <str>
        """
        while True:
            token = prefix + projex.security.generateToken() + suffix
            if len(cls.select(where=Q(column) == token)) == 0:
                return token

    @classmethod
    def getDatabase(cls):
        """
        Returns the database instance for this class.
        
        :return     <Database> || None
        """
        db = cls.__db__
        if isinstance(db, orb.Database):
            return db
        elif db:
            return orb.system.database(db)
        else:
            return cls.schema().database()

    @classmethod
    def getRecord(cls, key, **options):
        if 'where' not in options:
            if type(key) in (str, unicode):
                try:
                    key = int(key)
                except StandardError:
                    raise orb.errors.RecordNotFound(cls, key)
            options['where'] = Q(cls) == key
        return cls.selectFirst(**options)

    @staticmethod
    def groupRecords(records, groupings, inflated=False):
        """
        Creates a grouping of the records based on the inputted columns.  You \
        can supply as many column values as you'd like creating nested \
        groups.
        
        :param      records     | <Table>
                    groupings     | [<str>, ..]
                    inflated | <bool>
        
        :return     <dict>
        """
        if inflated is None:
            inflated = True

        output = {}
        ref_cache = {}  # stores the grouping options for auto-inflated vars

        for record in records:
            data = output
            schema = record.schema()

            # make sure we have the proper level
            for i in range(len(groupings) - 1):
                grouping_key = Table.__groupingKey(record,
                                                   schema,
                                                   groupings[i],
                                                   ref_cache,
                                                   inflated)

                data.setdefault(grouping_key, {})
                data = data[grouping_key]

            grouping_key = Table.__groupingKey(record,
                                               schema,
                                               groupings[-1],
                                               ref_cache,
                                               inflated)

            data.setdefault(grouping_key, [])
            data[grouping_key].append(record)

        return output

    @classmethod
    def inflateRecord(cls, values, **options):
        """
        Returns a new record instance for the given class with the values
        defined from the database.
        
        :param      cls     | <subclass of orb.Table>
                    values  | <dict> values
        
        :return     <orb.Table>
        """
        lookup = orb.LookupOptions(**options)
        context = orb.ContextOptions(**options)

        # inflate values from the database into the given class type
        if Table.recordcheck(values):
            record = values
            values = dict(values)
        else:
            record = None

        schema = cls.schema()
        column = schema.polymorphicColumn()

        # attempt to expand the class to its defined polymorphic type
        if column and column.fieldName() in values:
            # expand based on SQL style reference inheritance
            morph = column.referenceModel()
            if morph:
                morph_name = nstr(morph(values[column.fieldName()], options=context))
                dbname = cls.schema().databaseName()
                morph_cls = orb.system.model(morph_name, database=dbname)

                if morph_cls and morph_cls != cls:
                    pcols = morph_cls.schema().primaryColumns()
                    pkeys = [values.get(pcol.name(), values.get(pcol.fieldName())) for pcol in pcols if pcol.fieldName() in values]
                    record = morph_cls(*pkeys, options=context)
                elif not morph_cls:
                    raise orb.errors.TableNotFound(morph_name)

            # expand based on postgres-style OO inheritance
            elif column.isString():
                table_type = column.fieldName()
                dbname = cls.schema().databaseName()
                morph_cls = orb.system.model(values[table_type], database=dbname)
                if morph_cls and morph_cls != cls:
                    pcols = morph_cls.schema().primaryColumns()
                    pkeys = [values.get(pcol.name(), values.get(pcol.fieldName())) for pcol in pcols if pcol.fieldName() in values]
                    record = morph_cls(*pkeys, options=context)
                elif not morph_cls:
                    raise orb.errors.TableNotFound(table_type)

        if record is None:
            record = cls(__values=values, options=context)

        return record

    @classmethod
    def markTableCacheExpired(cls):
        """
        Marks the current date time as the latest time that the cache
        needs to be updated from.
        """
        cache = cls.tableCache()
        if cache:
            cache.expire()

    @classmethod
    def polymorphicModel(cls, key, default=None):
        """
        Returns the polymorphic reference model for this table.  This allows
        a table to reference external links through the inputted key.
        
        :param      key | <str>
                    default | <variant>
        
        :return     <subclass of orb.Table> || None
        """
        models = getattr(cls, '_%s__models' % cls.__name__, {})
        if key in models:
            return models[key]

        classes = getattr(cls, '_%s__polymorphs' % cls.__name__, {})
        name = classes.get(key, key)
        if name:
            model = orb.system.model(name)
            models[key] = model
            setattr(cls, '_%s__models' % cls.__name__, models)
            return model
        return None

    @classmethod
    def proxyColumn(cls, name):
        """
        Returns a column that is treated as a proxy for this widget.
        
        :param      name | <str>
        
        :return     <orb.Column> || None
        """
        return getattr(cls, '_%s__proxies' % cls.__name__, {}).get(nstr(name))

    @classmethod
    def proxyColumns(cls):
        """
        Returns a dictionary of proxy columns for this class type.  Proxy
        columns are dynamic methods that can be treated as common columns of
        data.
        
        :return     {<str> columnName: <orb.Column>, ..}
        """
        return getattr(cls, '_%s__proxies' % cls.__name__, {}).values()

    @classmethod
    def popRecordCache(cls):
        """
        Pops the last cache instance from the table.
        
        :return     <orb.RecordCache> || None
        """
        stack = getattr(cls, '_%s__recordCacheStack' % cls.__name__, [])
        if stack:
            return stack.pop()
        return None

    @classmethod
    def pushRecordCache(cls, cache):
        """
        Pushes a caching class onto the stack for this table.
        
        :param      cache | <orb.RecordCache>
        """
        stack = getattr(cls, '_%s__recordCacheStack' % cls.__name__, None)
        if stack is None:
            stack = []
            setattr(cls, '_%s__recordCacheStack' % cls.__name__, stack)

        stack.append(cache)

    @classmethod
    def recordCache(cls):
        """
        Returns the record cache for the inputted class.  If the given class
        schema does not define caching, then a None value is returned, otherwise
        a <orb.RecordCache> instance is returned.
        
        :return     <orb.RecordCache> || None
        """
        stack = getattr(cls, '_%s__recordCacheStack' % cls.__name__, [])
        if stack:
            return stack[-1]

        # checks to see if the schema defines a cache
        schema = cls.schema()

        # define the cache for the first time
        cache = orb.RecordCache(cls)
        cache.setTimeout(cls, schema.cacheTimeout() if schema.isCacheEnabled() else 1)
        cls.pushRecordCache(cache)

        return cache

    @classmethod
    def resolveQueryValue(cls, value):
        """
        Allows for class-level definitions for creating custom query options.
        
        :param      value | <variant>
        
        :return     <variant>
        """
        return value

    @classmethod
    def schema(cls):
        """  Returns the class object's schema information. """
        return cls.__db_schema__

    @classmethod
    def searchThesaurus(cls):
        """
        Returns the search thesaurus for this class.
        
        :return     <orb.SearchThesaurus>
        """
        key = '_{0}__searchThesaurus'.format(cls.__name__)
        return getattr(cls, key, orb.system.searchThesaurus())

    @classmethod
    def selectFirst(cls, *args, **kwds):
        """
        Selects records for the class based on the inputted \
        options.  If no db is specified, then the current \
        global database will be used.  If the inflated flag is specified, then \
        the results will be inflated to class instances.  If the flag is left \
        as None, then results will be auto-inflated if no columns were supplied.
        If columns were supplied, then the results will not be inflated by \
        default.
        
        :sa     select
        
        :return     <cls> || None
        """
        return cls.select().first(*args, **kwds)

    @classmethod
    def select(cls, *args, **kwds):
        """
        Selects records for the class based on the inputted \
        options.  If no db is specified, then the current \
        global database will be used.  If the inflated flag is specified, then \
        the results will be inflated to class instances.  
        
        If the flag is left as None, then results will be auto-inflated if no 
        columns were supplied.  If columns were supplied, then the results will 
        not be inflated by default.
        
        If the groupBy flag is specified, then the groupBy columns will be added
        to the beginning of the ordered search (to ensure proper paging).  See
        the Table.groupRecords methods for more details.
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, as well as the keyword 'lookup' to
                    an instance of <orb.LookupOptions> and 'options' for 
                    an instance of the <orb.ContextOptions>
        
        :return     [ <cls>, .. ] || { <variant> grp: <variant> result, .. }
        """
        db = kwds.pop('db', None)

        # support legacy code
        arg_headers = ['columns', 'where', 'order', 'limit']
        for i in range(len(args)):
            if i == 0 and isinstance(args[i], orb.LookupOptions):
                kwds['lookup'] = args[i]
            elif i == 1 and isinstance(args[i], orb.ContextOptions):
                kwds['options'] = args[i]
            else:
                kwds[arg_headers[i]] = args[i]

        lookup = orb.LookupOptions(**kwds)
        lookup.order = lookup.order or cls.schema().defaultOrder()
        options = orb.ContextOptions(**kwds)

        # determine if we should auto-add locale
        if options.locale != 'all' and cls.schema().column('locale') and cls.schema().autoLocalize():
            if not (lookup.where and 'locale' in lookup.where):
                lookup.where = (orb.Query('locale') == options.locale) & lookup.where

        # define the record set and return it
        rset = orb.RecordSet(cls, None)
        rset.setLookupOptions(lookup)
        rset.setContextOptions(options)

        if db is not None:
            rset.setDatabase(db)

        terms = kwds.pop('terms', '')
        if terms:
            rset = rset.search(terms)

        if options.inflated:
            if options.context and 'RecordSet' in options.context:
                return options.context['RecordSet'](rset)
            return rset
        else:
            return rset.records()

    @classmethod
    def tableCache(cls):
        """
        Returns the cache for this table from its schema.

        :return     <orb.caching.TableCache>
        """
        key = '_{0}__table_cache'.format(cls.__name__)
        try:
            return getattr(cls, key)
        except AttributeError:
            if cls.schema().isCacheEnabled():
                cache = orb.TableCache(cls, cls.schema().cache(), timeout=cls.schema().cacheTimeout())
                setattr(cls, key, cache)
                return cache

    @classmethod
    def callbacks(cls):
        """
        Returns the callback set for this table type.

        :return     <projex.callbacks.CallbackSet>
        """
        key = '_{0}__callbacks'.format(cls.__name__)
        try:
            return getattr(cls, key)
        except AttributeError:
            callbacks = CallbackSet()
            setattr(cls, key, callbacks)
            return callbacks

    @classmethod
    def setBaseQuery(cls, query):
        """
        Sets the default table query value.  This method can be used to control
        all queries for a given table by setting global where inclusions.
        
        :param      query | <orb.Query> || None
        """
        setattr(cls, '_%s__baseQuery' % cls.__name__, query)

    @classmethod
    def setCurrentRecord(cls, record):
        """
        Sets the current record that was tagged for usage.
        
        :return     <orb.Table> || None
        """
        return setattr(cls, '_{0}__current'.format(cls.__name__), record)

    @classmethod
    def setPolymorphicModel(cls, key, value):
        """
        Sets the polymorphic model for this table to the inputted value.
        
        :param      key     | <str>
                    value   | <str>
        """
        classes = getattr(cls, '_%s__polymorphs' % cls.__name__, {})
        classes[key] = value
        setattr(cls, '_%s__polymorphs' % cls.__name__, classes)

    @classmethod
    def setSearchThesaurus(cls, thesaurus):
        """
        Sets the search thesaurus for this class.
        
        :param     thesaurus | <orb.SearchThesaurus>
        """
        key = '_{0}__searchThesaurus'.format(cls.__name__)
        setattr(cls, key, thesaurus)

    @classmethod
    def tableSubTypes(cls):
        """
        Returns a list of all the sub-models of this class within the system.
        
        :return     [<subclass of orb.Table>, ..]
        """
        models = orb.system.models()
        return [model for model in models if issubclass(model, cls)]

    @classmethod
    def recordcheck(cls, obj):
        """
        Checks to see if the inputted obj ia s Table record instance.
        
        :param      obj     | <variant>
        
        :return     <bool>
        """
        return isinstance(obj, cls)

    @classmethod
    def typecheck(cls, obj):
        """
        Checks to see if the inputted obj is a subclass of a table.
        
        :param      obj     |  <variant>
                    cls     |  <subclass of Table> || None
        
        :return     <bool>
        """
        try:
            return issubclass(obj, cls)
        except TypeError:
            return False
