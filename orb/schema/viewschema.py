""" Defines the meta information for a View class. """

import logging
import projex.text

from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree

from .meta.metaview import MetaView
from . import dynamic

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class ViewSchema(object):
    """
    Contains meta data information about a view as it maps to a database.
    """

    TEMPLATE_PREFIXED = '[prefix::underscore::lower]_[name::underscore::lower]'
    TEMPLATE_VIEW = '[name::underscore::lower]'

    _customHandlers = []

    def __cmp__(self, other):
        # check to see if this is the same instance
        if id(self) == id(other):
            return 0

        # make sure this instance is a valid one for the other kind
        if not isinstance(other, ViewSchema):
            return -1

        # compare inheritance level
        my_ancestry = self.ancestry()
        other_ancestry = other.ancestry()

        result = cmp(len(my_ancestry), len(other_ancestry))
        if not result:
            # compare groups
            my_group = self.group()
            other_group = other.group()

            if my_group is not None and other_group is not None:
                result = cmp(my_group.order(), other_group.order())
            elif my_group:
                result = -1
            else:
                result = 1

            if not result:
                return cmp(self.name(), other.name())
            return result
        return result

    def __init__(self, referenced=False):
        self._abstract = False
        self._autoPrimary = True
        self._name = ''
        self._databaseName = ''
        self._groupName = ''
        self._dbname = ''
        self._inherits = ''
        self._stringFormat = ''
        self._namespace = ''
        self._displayName = ''
        self._inheritedLoaded = False
        self._cache = None
        self._cacheTimeout = 0
        self._cacheEnabled = False
        self._preloadCache = False
        self._static = False
        self._columns = set()
        self._columnIndex = {}
        self._views = {}
        self._properties = {}
        self._indexes = []
        self._pipes = []
        self._contexts = {}
        self._database = None
        self._timezone = None
        self._defaultOrder = None
        self._group = None
        self._primaryColumns = None
        self._model = None
        self._archiveModel = None
        self._referenced = referenced
        self._searchEngine = None
        self._validators = []
        self._archived = False

    def addColumn(self, column):
        """
        Adds the inputted column to this view schema.

        :param      column  | <orb.Column>
        """
        self._columns.add(column)
        if not column._schema:
            column._schema = self

    def addIndex(self, index):
        """
        Adds the inputted index to this view schema.

        :param      index   | <orb.Index>
        """
        if index in self._indexes:
            return

        index.setSchema(self)
        self._indexes.append(index)

    def addPipe(self, pipe):
        """
        Adds the inputted pipe reference to this view schema.

        :param      pipe | <orb.Pipe>
        """
        if pipe in self._pipes:
            return

        pipe.setSchema(self)
        self._pipes.append(pipe)

    def addValidator(self, validator):
        """
        Adds a record validators that are associated with this schema.  You
        can define different validation addons for a view's schema that will process when
        calling the View.validateRecord method.

        :param      validator | <orb.AbstractRecordValidator>
        """
        self._validators.append(validator)

    def ancestor(self):
        """
        Returns the direct ancestor for this schema that it inherits from.

        :return     <ViewSchema> || None
        """
        if self.inherits():
            return orb.system.schema(self.inherits())
        return None

    def ancestry(self):
        """
        Returns the different inherited schemas for this instance.

        :return     [<ViewSchema>, ..]
        """
        if not self._inherits:
            return []

        schema = orb.system.schema(self.inherits())
        if not schema:
            return []

        return schema.ancestry() + [schema]

    def ancestryQuery(self):
        """
        Returns the query that will link this schema to its ancestors.

        :return     <Query> || <QueryCompound>
        """
        if not self.inherits():
            return None

        from orb import Query as Q

        query = Q()
        ancestry = self.ancestry()
        schema = self

        ifield_templ = orb.system.settings().inheritField()
        for ancest in reversed(ancestry):
            afield = ifield_templ.format(view=ancest.databaseName())
            sfield = ifield_templ.format(view=schema.databaseName())

            if ancest.inherits():
                query &= Q(ancest.model(), afield) == Q(schema.model(), sfield)
            else:
                query &= Q(ancest.model()) == Q(schema.model(), sfield)

            schema = ancest
        return query

    def ancestryModels(self):
        """
        Returns the ancestry models for this schema.

        :return     [<subclass of orb.View>, ..]
        """
        return [x.model() for x in self.ancestry()]

    def archiveModel(self):
        return self._archiveModel

    def autoPrimary(self):
        """
        Returns whether or not this schema auto-generates the primary key.  \
        This is useful when defining reusable schemas that could be applied \
        to various backends, for instance an auto-increment column for a \
        PostgreSQL database vs. a string column for a MongoDB.  By default, it \
        is recommended this value remain True.

        :return     <bool>
        """
        return self._autoPrimary

    def baseAncestor(self):
        """
        Returns the base ancestor for this schema.

        :return     <orb.ViewSchema>
        """
        if not self.inherits():
            return self

        ancestry = self.ancestry()
        if ancestry:
            return ancestry[0]
        return self

    def cache(self):
        """
        Returns the table cache associated with this schema.

        :return     <orb.caching.TableCache>
        """
        return self._cache or self.database().cache()

    def cacheTimeout(self):
        """
        Returns the number of seconds that the caching system will use before
        clearing its cache data.

        :return     <int> | seconds
        """
        return self._cacheTimeout

    def column(self, name, recurse=True, flags=0, kind=0):
        """
        Returns the column instance based on its name.
        If error reporting is on, then the ColumnNotFound
        error will be thrown the key inputted is not a valid
        column name.

        :param      name | <str>
                    recurse | <bool>
                    flags | <int>
                    kind | <int>

        :return     <orb.Column> || None
        """
        key = (name, recurse, flags, kind)
        key = hash(key)

        # lookup the existing cached record
        try:
            return self._columnIndex[key]
        except KeyError:
            pass

        # lookup the new record
        parts = nstr(name).split('.')
        part = parts[0]
        next_parts = parts[1:]

        # generate the primary columns
        found = None
        for column in self.columns(recurse=recurse, flags=flags, kind=kind):
            if column.isMatch(part):
                found = column
                break

        # lookup referenced joins
        if found is not None and next_parts:
            refmodel = found.referenceModel()
            if refmodel:
                refschema = refmodel.schema()
                next_part = '.'.join(next_parts)
                found = refschema.column(next_part, recurse=recurse, flags=flags, kind=kind)
            else:
                found = None

        # cache this lookup for the future
        self._columnIndex[key] = found
        return found

    def columnNames(self, recurse=True, flags=0, kind=0):
        """
        Returns the list of column names that are defined for
        this view schema instance.

        :return     <list> [ <str> columnName, .. ]
        """
        return sorted([x.name() for x in self.columns(recurse=recurse, flags=flags, kind=kind)])

    def columns(self, recurse=True, flags=0, kind=0):
        """
        Returns the list of column instances that are defined
        for this view schema instance.

        :param      recurse | <bool>
                    flags   | <orb.Column.Flags>
                    kind    | <orb.Column.Kind>

        :return     <list> [ <orb.Column>, .. ]
        """
        # generate the primary columns for this schema
        output = {column for column in self._columns if
                  (not flags or column.testFlag(flags)) and
                  (not kind or column.isKind(kind))}

        if kind & orb.Column.Kind.Proxy and self._model:
            output.update(self._model.proxyColumns())

        return list(output)

    def context(self, name):
        """
        Returns the context for this schema.  This will define different override options when a particular
        call is made to the view within a given context.

        :param      name | <str>

        :return     <dict>
        """
        return self._contexts.get(name, {})

    def contexts(self):
        """
        Returns the full set of contexts for this schema.

        :return     {<str> context name: <dict> context, ..}
        """
        return self._contexts

    def databaseName(self):
        """
        Returns the name of the database that this schema will be linked to.

        :return     <str>
        """
        if self._databaseName:
            return self._databaseName
        else:
            try:
                return self.group().databaseName()
            except AttributeError:
                return ''

    def database(self):
        """
        Returns the database that is linked with the current schema.

        :return     <Database> || None
        """
        if self._database is not None:
            return self._database

        dbname = self.databaseName()
        if dbname:
            return orb.system.database(dbname)
        return orb.system.database()

    def defaultColumns(self):
        """
        Returns the columns that should be used by default when querying for
        this view.

        :return     [<orb.Column>, ..]
        """
        flag = orb.Column.Flags.IgnoreByDefault
        return [col for col in self.columns() if not col.testFlag(flag)]

    def defaultOrder(self):
        """
        Returns the default order to be used when querying this schema.

        :return     [(<str> columnName, <str> asc|desc), ..] || None
        """
        return self._defaultOrder

    def displayName(self):
        """
        Returns the display name for this view.

        :return     <str>
        """
        if not self._displayName:
            return projex.text.pretty(self.name())
        return self._displayName

    def fieldNames(self, recurse=True, flags=0, kind=0):
        """
        Returns the list of column instances that are defined
        for this view schema instance.

        :param      recurse | <bool>

        :return     <list> [ <str>, .. ]
        """
        return [col.fieldName() for col in self.columns(recurse=recurse, flags=flags, kind=kind)]

    def generateModel(self):
        """
        Generates the default model class for this view schema, if no \
        default model already exists.  The new generated view will be \
        returned.

        :return     <subclass of View> || None
        """
        if self._model:
            return self._model

        # generate the base models
        if self.inherits():
            inherits = self.inherits()
            inherited = orb.system.schema(inherits)

            if not inherited:
                log.error('Could not find inherited model: %s', inherits)
                base = None
            else:
                base = inherited.model(autoGenerate=True)

            if base:
                bases = [base]
            else:
                bases = [orb.system.baseViewType()]
        else:
            bases = [orb.system.baseViewType()]

        # generate the attributes
        attrs = {'__db_schema__': self, '__module__': 'orb.schema.dynamic'}
        grp = self.group()
        prefix = ''
        if grp:
            prefix = grp.modelPrefix()

        cls = MetaView(prefix + self.name(), tuple(bases), attrs)
        setattr(dynamic, cls.__name__, cls)

        # generate archive layer
        if self.isArchived():
            # create the archive column
            archive_columns = []

            # create a duplicate of the existing columns, disabling translations since we'll store
            # a single record per change
            found_locale = False
            for column in self.columns(recurse=False, flags=~orb.Column.Flags.Primary):
                new_column = column.copy()
                new_column.setTranslatable(False)
                archive_columns.append(new_column)
                if column.name() == 'locale':
                    found_locale = True

            archive_columns += [
                # primary key for the archives is a reference to the article
                orb.Column(orb.ColumnType.ForeignKey,
                           self.name(),
                           fieldName='{0}_archived_id'.format(projex.text.underscore(self.name())),
                           required=True,
                           reference=self.name(),
                           reversed=True,
                           reversedName='archives'),

                # and its version
                orb.Column(orb.ColumnType.Integer,
                           'archiveNumber',
                           required=True),

                # created the archive at method
                orb.Column(orb.ColumnType.DatetimeWithTimezone,
                           'archivedAt',
                           default='now')
            ]

            # store data per locale
            if not found_locale:
                archive_columns.append(orb.Column(orb.ColumnType.String,
                                                  'locale',
                                                  fieldName='locale',
                                                  required=True,
                                                  maxLength=5))

            archive_data = {
                '__db__': self.databaseName(),
                '__db_group__': self.groupName(),
                '__db_name__': '{0}Archive'.format(self.name()),
                '__db_dbname__': '{0}_archives'.format(projex.text.underscore(self.name())),
                '__db_columns__': archive_columns,
                '__db_pipes__': [],
                '__db_schema__': None,
                '__db_abstract__': False,
                '__db_inherits__': None,
                '__db_autoprimary__': True,
                '__db_archived__': False
            }

            archive_class = MetaView(archive_data['__db_name__'], (orb.View,), archive_data)
            archive_schema = archive_class.schema()
            archive_schema.setDefaultOrder([('archiveNumber', 'asc')])
            self.setArchiveModel(archive_class)
            setattr(dynamic, archive_class.__name__, archive_class)

        return cls

    def generatePrimary(self):
        """
        Auto-generates the primary column for this schema based on the \
        current settings.

        :return     [<orb.Column>, ..] || None
        """
        if orb.system.settings().editOnlyMode():
            return None

        db = self.database()
        if not (db and db.backend()):
            return None

        # create the default primary column from the inputted type
        return [db.backend().defaultPrimaryColumn()]

    def group(self):
        """
        Returns the schema group that this schema is related to.

        :return     <orb.ViewGroup> || None
        """
        if not self._group:
            dbname = self._databaseName
            grp = orb.system.group(self.groupName(), database=dbname)
            self._group = grp

        return self._group

    def groupName(self):
        """
        Returns the name of the group that this schema is a part of in the \
        database.

        :return     <str>
        """
        if self._group:
            return self._group.name()
        return self._groupName

    def hasColumn(self, column, recurse=True, flags=0, kind=0):
        """
        Returns whether or not this column exists within the list of columns
        for this schema.

        :return     <bool>
        """
        return column in self.columns(recurse=recurse, flags=flags, kind=kind)

    def hasTranslations(self):
        for col in self.columns():
            if col.isTranslatable():
                return True
        return False

    def indexes(self, recurse=True):
        """
        Returns the list of indexes that are associated with this schema.

        :return     [<orb.Index>, ..]
        """
        return self._indexes[:]

    def inherits(self):
        """
        Returns the name of the view schema that this class will inherit from.

        :return     <str>
        """
        return self._inherits

    def inheritsModel(self):
        if not self.inherits():
            return None
        return orb.system.model(self.inherits())

    def inheritsRecursive(self):
        """
        Returns all of the views that this view inherits from.

        :return     [<str>, ..]
        """
        output = []
        inherits = self.inherits()

        while inherits:
            output.append(inherits)

            view = orb.system.schema(inherits)
            if not view:
                break

            inherits = view.inherits()

        return output

    def isAbstract(self):
        """
        Returns whether or not this schema is an abstract view.  Abstract \
        views will not register to the database, but will serve as base \
        classes for inherited views.

        :return     <bool>
        """
        return self._abstract

    def isArchived(self):
        """
        Returns whether or not this schema is archived.  Archived schema's will store additional records
        each time a record is created or updated for historical reference.

        :return     <bool>
        """
        return self._archived

    def isCacheEnabled(self):
        """
        Returns whether or not caching is enabled for this View instance.

        :sa     setCacheEnabled

        :return     <bool>
        """
        return self._cacheEnabled

    def isReferenced(self):
        """
        Returns whether or not this schema is referenced from an external file.

        :return     <bool>
        """
        return self._referenced

    def isStatic(self):
        return self._static

    def model(self, autoGenerate=False):
        """
        Returns the default View class that is associated with this \
        schema instance.

        :param      autoGenerate | <bool>

        :return     <subclass of View>
        """
        if self._model is None and autoGenerate:
            self._model = self.generateModel()

        return self._model

    def name(self):
        """
        Returns the name of this schema object.

        :return     <str>
        """
        return self._name

    def namespace(self):
        """
        Returns the namespace of this schema object.  If no namespace is
        defined, then its group namespace is utilized.

        :return     <str>
        """
        if self._namespace:
            return self._namespace

        grp = self.group()
        if grp:
            return grp.namespace()
        else:
            db = self.database()
            if db:
                return db.namespace()

            return orb.system.namespace()

    def polymorphicColumn(self):
        """
        Returns the first column that defines the polymorphic view type
        for this view.

        :return     <orb.Column> || None
        """
        for column in self.columns():
            if column.isPolymorphic():
                return column
        return None

    def pipe(self, name):
        """
        Returns the pipe that matches the inputted name.

        :return     <orb.Pipe> || None
        """
        for pipe in self._pipes:
            if pipe.name() == name:
                return pipe
        return None

    def pipes(self, recurse=True):
        """
        Returns a list of the pipes for this instance.

        :return     [<orb.Pipe>, ..]
        """
        return self._pipes[:]

    def preloadCache(self):
        """
        Returns whether or not this cache will be preloaded into memory.

        :return     <bool>
        """
        return self._preloadCache

    def primaryColumn(self):
        """
        Returns the primary column for this table.  If this table has no or multiple primary columns defined,
        then a PrimaryKeyNotDefined will be raised, otherwise the first column will be returned.

        :return     <orb.Column>
        """
        cols = self.primaryColumns()
        if len(cols) == 1:
            return cols[0]
        else:
            raise orb.PrimaryKeyNotDefined(self)

    def primaryColumns(self, db=None):
        """
        Returns the primary key columns for this view's
        schema.

        :return     <tuple> (<orb.Column>,)
        """
        return [col for col in self._columns if col.primary()]

    def property(self, key, default=None):
        """
        Returns the custom data that was stored on this view at the inputted \
        key.  If the key is not found, then the default value will be returned.

        :param      key         | <str>
                    default     | <variant>

        :return     <variant>
        """
        return self._properties.get(nstr(key), default)

    def removeColumn(self, column):
        """
        Removes the inputted column from this view's schema.

        :param      column | <orb.Column>
        """
        try:
            self._columns.remove(column)
            column._schema = None
        except ValueError:
            pass

    def removeIndex(self, index):
        """
        Removes the inputted index from this view's schema.

        :param      index | <orb.Index>
        """
        try:
            self._indexes.remove(index)
        except ValueError:
            pass

    def removePipe(self, pipe):
        """
        Removes the inputted pipe from this view's schema.

        :param      pipe | <orb.Pipe>
        """
        try:
            self._pipes.remove(pipe)
        except ValueError:
            pass

    def removeValidator(self, validator):
        """
        Removes a record validators that are associated with this schema.  You
        can define different validation addons for a view's schema that will process when
        calling the View.validateRecord method.

        :param      validator | <orb.AbstractRecordValidator>
        """
        try:
            self._validators.remove(validator)
        except ValueError:
            pass

    def reverseLookup(self, name):
        """
        Returns the reverse lookup that matches the inputted name.

        :return     <orb.Column> || None
        """
        return {column.reversedName(): column for schema in orb.system.schemas()
                for column in schema.columns()
                if column.reference() == self.name() and column.isReversed()}.get(name)

    def reverseLookups(self):
        """
        Returns a list of all the reverse-lookup columns that reference this schema.

        :return     [<orb.Column>, ..]
        """
        return [column for schema in orb.system.schemas()
                for column in schema.columns()
                if column.reference() == self.name() and column.isReversed()]

    def searchEngine(self):
        """
        Returns the search engine that will be used for this system.

        :return     <orb.SearchEngine>
        """
        if self._searchEngine:
            return self._searchEngine
        else:
            return orb.system.searchEngine()

    def searchableColumns(self, recurse=True, flags=0, kind=0):
        """
        Returns a list of the searchable columns for this schema.

        :return     <str>
        """
        return self.columns(recurse=recurse, flags=flags | orb.Column.Flags.Searchable, kind=kind)

    def setAbstract(self, state):
        """
        Sets whether or not this view is abstract.

        :param      state | <bool>
        """
        self._abstract = state

    def setAutoPrimary(self, state):
        """
        Sets whether or not this schema will use auto-generated primary keys.

        :sa         autoPrimary

        :return     <bool>
        """
        self._autoPrimary = state

    def setArchived(self, state=True):
        """
        Sets the archive state for this schema.

        :param      state | <bool>
        """
        self._archived = state

    def setArchiveModel(self, model):
        self._archiveModel = model

    def setCache(self, cache):
        """
        Sets the table cache for this instance.

        :param      cache | <orb.caching.TableCache>
        """
        self._cache = cache

    def setCacheEnabled(self, state):
        """
        Sets whether or not to enable caching on the View instance this schema
        belongs to.  When caching is enabled, all the records from the view
        database are selected the first time a select is called and
        then subsequent calls to the database are handled by checking what is
        cached in memory.  This is useful for small views that don't change
        often (such as a Status or Type view) and are referenced frequently.

        To have the cache clear automatically after a number of minutes, set the
        cacheTimeout method.

        :param      state | <bool>
        """
        self._cacheEnabled = state
        if self._cache:
            self._cache.setEnabled(state)

    def setCacheTimeout(self, seconds):
        """
        Sets the number of seconds that the view should clear its cached
        results from memory and re-query the database.  If the value is 0, then
        the cache will have to be manually cleared.

        :param      seconds | <int> || <float>
        """
        self._cacheTimeout = seconds

    def setColumns(self, columns):
        """
        Sets the columns that this schema uses.

        :param      columns     | [<orb.Column>, ..]
        """
        self._columns = set(columns)

        # pylint: disable-msg=W0212
        for column in columns:
            if not column._schema:
                column._schema = self

    def setContext(self, name, context):
        """
        Sets the context for this schema.  This will define different override options when a particular
        call is made to the view within a given context.

        :param      name    | <str>
                    context | <dict>
        """
        self._contexts[name] = dict(context)

    def setContexts(self, contexts):
        """
        Sets the full context set for this view to the inputted dictionary of contexts.

        :param      contexts | {<str> context name: <dict>, ..}
        """
        self._contexts = dict(contexts)

    def setDefaultOrder(self, order):
        """
        Sets the default order for this schema to the inputted order.  This
        will be used when an individual query for this schema does not specify
        an order explicitly.

        :param     order | [(<str> columnName, <str> asc|desc), ..] || None
        """
        self._defaultOrder = order

    def setProperty(self, key, value):
        """
        Sets the custom data at the given key to the inputted value.

        :param      key     | <str>
                    value   | <variant>
        """
        self._properties[nstr(key)] = value

    def setDatabase(self, database):
        """
        Sets the database name that this schema will be linked to.

        :param      database | <orb.Database> || <str> || None
        """
        if isinstance(database, orb.Database):
            self._database = database
        elif database is None:
            self._database = None
        else:
            self._database = None
            self.setDatabaseName(database)

    def setDatabaseName(self, databaseName):
        """
        Sets the database name that this schema will be linked to.

        :param      databaseName | <str>
        """
        self._databaseName = nstr(databaseName)

    def setDisplayName(self, name):
        """
        Sets the display name for this view.

        :param      name | <str>
        """
        self._displayName = name

    def setModel(self, model):
        """
        Sets the default View class that is associated with this \
        schema instance.

        :param    model     | <subclass of View>
        """
        self._model = model

    def setNamespace(self, namespace):
        """
        Sets the namespace that will be used for this schema to the inputted
        namespace.

        :param      namespace | <str>
        """
        self._namespace = namespace

    def setIndexes(self, indexes):
        """
        Sets the list of indexed lookups for this schema to the inputted list.

        :param      indexes     | [<orb.Index>, ..]
        """
        self._indexes = indexes[:]

    def setInherits(self, name):
        """
        Sets the name for the inherited view schema to the inputted name.

        :param      name    | <str>
        """
        self._inherits = name

    def setName(self, name):
        """
        Sets the name of this schema object to the inputted name.

        :param      name    | <str>
        """
        self._name = name

    def setGroup(self, group):
        """
        Sets the group association for this schema to the inputted group.

        :param      group | <orb.ViewGroup>
        """
        self._group = group
        self._groupName = group.name() if group else ''

    def setGroupName(self, groupName):
        """
        Sets the group name that this view schema will be apart of.

        :param      groupName   | <str>
        """
        self._groupName = groupName

    def setPipes(self, pipes):
        """
        Sets the pipe methods that will be used for this schema.

        :param      pipes | [<orb.Pipes>, ..]
        """
        self._pipes = pipes

    def setPreloadCache(self, state):
        """
        Sets whether or not to preload all records for this view as
        a cache.

        :param      state | <bool>
        """
        self._preloadCache = state

    def setSearchEngine(self, engine):
        """
        Sets the search engine that will be used for this system.

        :param      engine | <orb.SearchEngine>
        """
        self._searchEngine = engine

    def setStatic(self, static):
        self._static = static

    def setStringFormat(self, text):
        """
        Sets a string format to be used when rendering a view using the str()
        method.  This is a python string format with dictionary keys for the
        column values that you want to display.

        :param      format | <str>
        """
        self._stringFormat = nstr(text)

    def setDbName(self, dbname):
        """
        Sets the name that will be used in the actual database.  If the \
        name supplied is blank, then the default database name will be \
        used based on the group and name for this schema.

        :param      dbname  | <str>
        """
        self._dbname = dbname

    def setTimezone(self, timezone):
        """
        Sets the timezone associated directly to this database.

        :sa     <orb.Orb.setTimezone>

        :param     timezone | <pytz.tzfile> || None
        """
        self._timezone = timezone

    def setValidators(self, validators):
        """
        Sets the list of the record validators that are associated with this schema.  You
        can define different validation addons for a view's schema that will process when
        calling the View.validateRecord method.

        :param      validators | [<orb.AbstractRecordValidator>, ..]
        """
        self._validators = validators

    def setView(self, name, view):
        """
        Adds a new view to this schema.  Views provide pre-built dynamically joined tables that can
        give additional information to a table.

        :param      name | <str>
                    view | <orb.View>
        """
        self._views[name] = view

    def stringFormat(self):
        """
        Returns the string format style for this schema.

        :return     <str>
        """
        return self._stringFormat

    def dbname(self):
        """
        Returns the name that will be used for the view in the database.

        :return     <str>
        """
        if not self._dbname:
            grp = self.group()
            prefix = grp.modelPrefix() if grp and grp.useModelPrefix() else ''
            self._dbname = self.defaultDbName(self.name(), prefix)
        return self._dbname

    def timezone(self, options=None):
        """
        Returns the timezone associated specifically with this database.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the Orb system in general.

        :sa     <orb.Orb>

        :return     <pytz.tzfile> || None
        """
        if self._timezone is None:
            return self.database().timezone(options)
        return self._timezone

    def toolTip(self, context='normal'):
        tip = '''\
<b>{name} <small>(View from {group} group)</small></b><br>
<em>Usage</em>
<pre>
>>> # api usage
>>> record = {name}()
>>> record.commit()

>>> all_records = {name}.all()
>>> some_records = {name}.select(where=query)

>>> # meta data
>>> schema = {name}.schema()

>>> # ui display info
>>> schema.displayName()
'{display}'

>>> # database view info
>>> schema.databaseName()
'{db_name}'
</pre>'''
        return tip.format(name=self.name(),
                          group=self.groupName(),
                          db_name=self.databaseName(),
                          display=self.displayName())

    def toXml(self, xparent=None):
        """
        Saves this schema information to XML.

        :param      xparent     | <xml.etree.ElementTree.Element>

        :return     <xml.etree.ElementTree.Element>
        """
        if xparent is not None:
            xschema = ElementTree.SubElement(xparent, 'schema')
        else:
            xschema = ElementTree.Element('schema')

        # save the properties
        xschema.set('name', self.name())
        if self.displayName() != projex.text.pretty(self.name()):
            xschema.set('displayName', self.displayName())
        if self.inherits():
            xschema.set('inherits', self.inherits())
        if self.dbname() != projex.text.underscore(projex.text.pluralize(self.name())):
            xschema.set('dbname', self.dbname())
        if not self.autoPrimary():
            xschema.set('autoPrimary', nstr(self.autoPrimary()))
        if self.isCacheEnabled():
            xschema.set('cacheEnabled', nstr(self.isCacheEnabled()))
            xschema.set('cacheTimeout', nstr(self._cacheTimeout))
            xschema.set('preloadCache', nstr(self.preloadCache()))

        if self.stringFormat():
            xschema.set('stringFormat', self.stringFormat())
        if self.isArchived():
            xschema.set('archived', nstr(self.isArchived()))

        # save the properties
        if self._properties:
            xprops = ElementTree.SubElement(xschema, 'properties')
            for prop, value in sorted(self._properties.items()):
                xprop = ElementTree.SubElement(xprops, 'property')
                xprop.set('key', nstr(prop))
                xprop.set('value', nstr(value))

        # save the columns
        for column in sorted(self.columns(recurse=False),
                             key=lambda x: x.name()):
            column.toXml(xschema)

        # save the indexes
        for index in sorted(self.indexes(), key=lambda x: x.name()):
            index.toXml(xschema)

        # save the pipes
        for pipe in sorted(self.pipes(), key=lambda x: x.name()):
            pipe.toXml(xschema)

        return xschema

    def validators(self):
        """
        Returns a list of the record validators that are associated with this schema.  You
        can define different validation addons for a view's schema that will process when
        calling the View.validateRecord method.

        :return     [<orb.AbstractRecordValidator>, ..]
        """
        return self._validators

    def view(self, name):
        """
        Returns the view for this schema that matches the given name.

        :return     <orb.View> || None
        """
        return self._views.get(name)

    @staticmethod
    def defaultDbName(name, prefix=''):
        """
        Returns the default database view name for the inputted name \
        and prefix.

        :param      name    | <str>
        :param      prefix  | <str>
        """
        if prefix:
            templ = ViewSchema.TEMPLATE_PREFIXED
        else:
            templ = ViewSchema.TEMPLATE_VIEW

        options = {'name': name, 'prefix': prefix}

        return projex.text.render(templ, options)

    @staticmethod
    def fromXml(xschema, referenced=False):
        """
        Generates a new view schema instance for the inputted database schema \
        based on the given xml information.

        :param      xschema      | <xml.etree.Element>

        :return     <ViewSchema> || None
        """
        tschema = orb.ViewSchema(referenced=referenced)

        # load the properties
        tschema.setName(xschema.get('name', ''))
        tschema.setDisplayName(xschema.get('displayName', ''))
        tschema.setGroupName(xschema.get('group', ''))
        tschema.setInherits(xschema.get('inherits', ''))
        tschema.setDbName(xschema.get('dbname', ''))
        tschema.setAutoPrimary(xschema.get('autoPrimary') != 'False')
        tschema.setStringFormat(xschema.get('stringFormat', ''))
        tschema.setCacheEnabled(xschema.get('cacheEnabled') == 'True')

        # support the cacheExpire key that was stored as minutes, as of ORB 4.4.0, all cache
        # timeouts are in seconds
        tschema.setCacheTimeout(int(xschema.get('cacheTimeout', int(xschema.get('cacheExpire', 0)) * 60)))

        tschema.setPreloadCache(xschema.get('preloadCache') == 'True')
        tschema.setArchived(xschema.get('archived') == 'True')

        # load the properties
        xprops = xschema.find('properties')
        if xprops is not None:
            for xprop in xprops:
                tschema.setProperty(xprop.get('key'), xprop.get('value'))

        # load the columns
        for xcolumn in xschema.findall('column'):
            column = orb.Column.fromXml(xcolumn, referenced)
            if column:
                tschema.addColumn(column)

        # load the indexes
        for xindex in xschema.findall('index'):
            index = orb.Index.fromXml(xindex, referenced)
            if index:
                tschema.addIndex(index)

        # load the pipes
        for xpipe in xschema.findall('pipe'):
            pipe = orb.Pipe.fromXml(xpipe, referenced)
            if pipe:
                tschema.addPipe(pipe)

        return tschema

