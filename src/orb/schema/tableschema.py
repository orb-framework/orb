#!/usr/bin/python

""" Defines the meta information for a Table class. """

# define authorship information
__authors__ = ['Eric Hulser']
__author__ = ','.join(__authors__)
__credits__ = []
__copyright__ = 'Copyright (c) 2011, Projex Software'
__license__ = 'LGPL'

# maintanence information
__maintainer__ = 'Projex Software'
__email__ = 'team@projexsoftware.com'

# ------------------------------------------------------------------------------

import logging
import projex.text

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from xml.etree import ElementTree

from .tablebase import TableBase
from . import dynamic

log = logging.getLogger(__name__)
orb = LazyModule('orb')
errors = LazyModule('orb.errors')


class TableSchema(object):
    """ 
    Contains meta data information about a table as it maps to a database.
    """

    TEMPLATE_PREFIXED = '[prefix::underscore::lower]_[name::underscore::lower]'
    TEMPLATE_TABLE = '[name::underscore::plural::lower]'

    _customHandlers = []

    def __cmp__(self, other):
        # check to see if this is the same instance
        if id(self) == id(other):
            return 0

        # make sure this instance is a valid one for the other kind
        if not isinstance(other, TableSchema):
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
        self._tableName = ''
        self._inherits = ''
        self._stringFormat = ''
        self._namespace = ''
        self._displayName = ''
        self._cacheExpireIn = 0
        self._useAdvancedFormatting = False
        self._inheritedLoaded = False
        self._cacheEnabled = False
        self._preloadCache = True
        self._columns = set()
        self._columnIndex = {}
        self._properties = {}
        self._indexes = []
        self._pipes = []
        self._database = None
        self._timezone = None
        self._defaultOrder = None
        self._group = None
        self._primaryColumns = None
        self._model = None
        self._referenced = referenced
        self._searchEngine = None

    def addColumn(self, column):
        """
        Adds the inputed column to this table schema.
        
        :param      column  | <orb.Column>
        """
        self._columns.add(column)
        if not column._schema:
            column._schema = self

    def addIndex(self, index):
        """
        Adds the inputed index to this table schema.
        
        :param      index   | <orb.Index>
        """
        if index in self._indexes:
            return

        index.setSchema(self)
        self._indexes.append(index)

    def addPipe(self, pipe):
        """
        Adds the inputed pipe reference to this table schema.
        
        :param      pipe | <orb.Pipe>
        """
        if pipe in self._pipes:
            return

        pipe.setSchema(self)
        self._pipes.append(pipe)

    def ancestor(self):
        """
        Returns the direct ancestor for this schema that it inherits from.
        
        :return     <TableSchema> || None
        """
        if self.inherits():
            return orb.system.schema(self.inherits())
        return None

    def ancestry(self):
        """
        Returns the different inherited schemas for this instance.
        
        :return     [<TableSchema>, ..]
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
            afield = ifield_templ.format(table=ancest.databaseName())
            sfield = ifield_templ.format(table=schema.databaseName())

            if ancest.inherits():
                query &= Q(ancest.model(), afield) == Q(schema.model(), sfield)
            else:
                query &= Q(ancest.model()) == Q(schema.model(), sfield)

            schema = ancest
        return query

    def ancestryModels(self):
        """
        Returns the ancestry models for this schema.
        
        :return     [<subclass of orb.Table>, ..]
        """
        return map(lambda x: x.model(), self.ancestry())

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
        
        :return     <orb.TableSchema>
        """
        if not self.inherits():
            return self

        ancestry = self.ancestry()
        if ancestry:
            return ancestry[0]
        return self

    def cacheExpireIn(self):
        """
        Returns the number of minutes that the caching system will use before
        clearing its cache data.
        
        :return     <int> | <float>
        """
        return self._cacheExpireIn

    def column(self,
               name,
               recurse=True,
               includeProxies=True,
               includeJoined=True,
               includeAggregates=True,
               traversal=None):
        """
        Returns the column instance based on its name.  
        If error reporting is on, then the ColumnNotFoundError 
        error will be thrown the key inputed is not a valid 
        column name.
        
        :param      name | <str>
                    recurse | <bool>
                    includeProxies | <bool>
                    traversal | <list> | will be populated with schemas
        
        :return     <orb.Column> || None
        """
        key = (name, recurse, includeProxies, includeJoined, includeAggregates)
        key = hash(key)

        # lookup the existing cached record
        try:
            found, traversed = self._columnIndex[key]
            if traversal is not None:
                traversal += traversed
            return found

        except KeyError:
            pass

        # lookup the new record
        parts = nstr(name).split('.')
        part = parts[0]
        next_parts = parts[1:]

        # generate the primary columns
        self.primaryColumns()
        found = None
        traversed = []
        for column in self.columns(recurse,
                                   includeProxies,
                                   includeJoined,
                                   includeAggregates):

            if column.isMatch(part):
                found = column
                break

        # lookup referenced joins
        if found is not None and next_parts:
            refmodel = found.referenceModel()
            if refmodel:
                # include the traversal information
                traversed.append(found)

                refschema = refmodel.schema()
                next_part = '.'.join(next_parts)
                found = refschema.column(next_part,
                                         recurse=recurse,
                                         includeProxies=includeProxies,
                                         includeJoined=includeJoined,
                                         includeAggregates=includeAggregates,
                                         traversal=traversed)

            else:
                found = None

        # cache this lookup for the future
        self._columnIndex[key] = (found, traversed)
        return found

    def columnNames(self,
                    recurse=True,
                    includeProxies=True,
                    includeJoined=True,
                    includeAggregates=True):
        """
        Returns the list of column names that are defined for 
        this table schema instance.
        
        :return     <list> [ <str> columnName, .. ]
        """
        cols = self.columns(recurse,
                            includeProxies,
                            includeJoined,
                            includeAggregates)
        return sorted(map(lambda x: x.name(), cols))

    def columns(self,
                recurse=True,
                includeProxies=True,
                includeJoined=True,
                includeAggregates=True,
                include=None,
                ignore=None):
        """
        Returns the list of column instances that are defined
        for this table schema instance.
        
        :param      recurse | <bool>
        
        :return     <list> [ <orb.Column>, .. ]
        """
        # generate the primary columns for this schema
        self.primaryColumns()

        output = self._columns.copy()
        if includeProxies and self._model:
            output.update(self._model.proxyColumns())

        if recurse:
            for ancest in self.ancestry():
                ancest.primaryColumns()
                ancest_columns = ancest._columns.copy()
                if includeProxies and ancest._model:
                    ancest_columns.update(ancest._model.proxyColumns())

                dups = output.intersection(ancest_columns)
                if dups:
                    dup_names = ','.join(map(lambda x: x.name(), dups))
                    raise errors.DuplicateColumnFound(self.name(), dup_names)

                output.update(ancest_columns)

        # filter the output based on the inputed list of names
        if ignore is not None:
            output = filter(lambda x: not x.name() in ignore and \
                                      not x.fieldName() in ignore and \
                                      not x.displayName() in ignore,
                            output)

        # filter the output based on the included list of names
        if include is not None:
            output = filter(lambda x: x.name() in include or \
                                      x.fieldName() in include or \
                                      x.displayName() in include,
                            output)

        # ignore joined columns
        if not includeJoined:
            output = filter(lambda x: not x.isJoined(), output)
        if not includeAggregates:
            output = filter(lambda x: not x.isAggregate(), output)

        return list(output)

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
        Returns the colum   ns that should be used by default when querying for
        this table.
        
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
        Returns the display name for this table.
        
        :return     <str>
        """
        if not self._displayName:
            return projex.text.pretty(self.name())
        return self._displayName

    def fieldNames(self,
                   recurse=True,
                   includeProxies=True,
                   includeJoined=True,
                   includeAggregates=True,
                   include=None,
                   ignore=None):
        """
        Returns the list of column instances that are defined
        for this table schema instance.
        
        :param      recurse | <bool>
        
        :return     <list> [ <str>, .. ]
        """
        columns = self.columns(recurse=recurse,
                               includeProxies=includeProxies,
                               includeJoined=includeJoined,
                               includeAggregates=includeAggregates)

        output = []
        for col in columns:
            if (include is not None and not col.fieldName() in include) or \
                    (ignore is not None and col.fieldName() in ignore):
                continue
            output.append(col.fieldName())
        return output

    def generateModel(self):
        """
        Generates the default model class for this table schema, if no \
        default model already exists.  The new generated table will be \
        returned.
        
        :return     <subclass of Table> || None
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
                bases = [orb.system.baseTableType()]
        else:
            bases = [orb.system.baseTableType()]

        # generate the attributes
        attrs = {'__db_schema__': self, '__module__': 'orb.schema.dynamic'}
        grp = self.group()
        prefix = ''
        if grp:
            prefix = grp.modelPrefix()

        cls = TableBase(prefix + self.name(), tuple(bases), attrs)
        setattr(dynamic, cls.__name__, cls)
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

        # create the default primary column from the inputed type
        return [db.backend().defaultPrimaryColumn()]

    def group(self):
        """
        Returns the schema group that this schema is related to.
        
        :return     <orb.TableGroup> || None
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

    def hasColumn(self, column, recurse=True, includeProxies=True):
        """
        Returns whether or not this column exists within the list of columns
        for this schema.
        
        :return     <bool>
        """
        # generate the primary columns for this schema
        self.primaryColumns()

        if column in self._columns:
            return True

        if includeProxies and self._model:
            if column in self._model.proxyColumns():
                return True

        if recurse:
            for ancest in self.ancestry():
                if ancest.hasColumn(column, recurse=False):
                    return True

        return False

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
        Returns the name of the table schema that this class will inherit from.
        
        :return     <str>
        """
        return self._inherits

    def inheritsModel(self):
        if not self.inherits():
            return None
        return orb.system.model(self.inherits())

    def inheritsRecursive(self):
        """
        Returns all of the tables that this table inherits from.
        
        :return     [<str>, ..]
        """
        output = []
        inherits = self.inherits()

        while inherits:
            output.append(inherits)

            table = orb.system.schema(inherits)
            if not table:
                break

            inherits = table.inherits()

        return output

    def isAbstract(self):
        """
        Returns whether or not this schema is an abstract table.  Abstract \
        tables will not register to the database, but will serve as base \
        classes for inherited tables.
        
        :return     <bool>
        """
        return self._abstract

    def isCacheEnabled(self):
        """
        Returns whether or not caching is enabled for this Table instance.
        
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

    def model(self, autoGenerate=False):
        """
        Returns the default Table class that is associated with this \
        schema instance.
        
        :param      autoGenerate | <bool>
        
        :return     <subclass of Table>
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
        Returns the first column that defines the polymorphic table type
        for this table.
        
        :return     <orb.Column> || None
        """
        for column in self.columns():
            if column.isPolymorphic():
                return column
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

    def primaryColumns(self, db=None):
        """
        Returns the primary key columns for this table's
        schema.
        
        :return     <tuple> (<orb.Column>,)
        """
        if self._primaryColumns:
            return self._primaryColumns

        if self.autoPrimary() and not self._inherits:
            cols = self.generatePrimary()

            if not cols is None:
                for col in cols:
                    self.addColumn(col)
        else:
            cols = None

        # generate the primary column list the first time
        if cols is None and self._inherits:
            inherited = orb.system.schema(self._inherits)
            if not inherited:
                raise errors.TableNotFound(self._inherits)
            else:
                cols = inherited.primaryColumns()

        if not cols is None:
            self._primaryColumns = cols[:]
            return self._primaryColumns
        else:
            return []

    def property(self, key, default=None):
        """
        Returns the custom data that was stored on this table at the inputed \
        key.  If the key is not found, then the default value will be returned.
        
        :param      key         | <str>
                    default     | <variant>
        
        :return     <variant>
        """
        return self._properties.get(nstr(key), default)

    def removeColumn(self, column):
        """
        Removes the inputed column from this table's schema.
        
        :param      column | <orb.Column>
        """
        try:
            self._columns.remove(column)
            column._schema = None
        except KeyError:
            pass

    def removeIndex(self, index):
        """
        Removes the inputed index from this table's schema.
        
        :param      index | <orb.Index>
        """
        if index in self._indexes:
            self._indexes.remove(index)

    def removePipe(self, pipe):
        """
        Removes the inputed pipe from this table's schema.
        
        :param      pipe | <orb.Pipe>
        """
        if pipe in self._pipes:
            self._pipes.remove(pipe)

    def searchEngine(self):
        """
        Returns the search engine that will be used for this system.
        
        :return     <orb.SearchEngine>
        """
        if self._searchEngine:
            return self._searchEngine
        else:
            return orb.system.searchEngine()

    def searchableColumns(self, recurse=True, includeProxies=True):
        """
        Returns a list of the searchable columns for this schema.
        
        :return     <str>
        """
        columns = self.columns(recurse, includeProxies)
        return filter(lambda x: x.isSearchable(), columns)

    def setAbstract(self, state):
        """
        Sets whether or not this table is abstract.
        
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

    def setCacheEnabled(self, state):
        """
        Sets whether or not to enable caching on the Table instance this schema
        belongs to.  When caching is enabled, all the records from the table
        database are selected the first time a select is called and
        then subsequent calls to the database are handled by checking what is
        cached in memory.  This is useful for small tables that don't change 
        often (such as a Status or Type table) and are referenced frequently.
        
        To have the cache clear automatically after a number of minutes, set the
        cacheExpireIn method.
        
        :param      state | <bool>
        """
        self._cacheEnabled = state

    def setCacheExpireIn(self, minutes):
        """
        Sets the number of minutes that the table should clear its cached
        results from memory and re-query the database.  If the value is 0, then
        the cache will have to be manually cleared.
        
        :param      minutes | <int> || <float>
        """
        self._cacheExpireIn = minutes

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

    def setDefaultOrder(self, order):
        """
        Sets the default order for this schema to the inputed order.  This
        will be used when an individual query for this schema does not specify
        an order explicitly.
        
        :param     order | [(<str> columnName, <str> asc|desc), ..] || None
        """
        self._defaultOrder = order

    def setProperty(self, key, value):
        """
        Sets the custom data at the given key to the inputed value.
        
        :param      key     | <str>
                    value   | <variant>
        """
        self._properties[nstr(key)] = value

    def setDatabase(self, database):
        """
        Sets the database name that this schema will be linked to.
        
        :param      database | <orb.Datatabase> || <str> || None
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
        Sets the display name for this table.
        
        :param      name | <str>
        """
        self._displayName = name

    def setModel(self, model):
        """
        Sets the default Table class that is associated with this \
        schema instance.
        
        :param    model     | <subclass of Table>
        """
        self._model = model

    def setNamespace(self, namespace):
        """
        Sets the namespace that will be used for this schema to the inputed
        namespace.
        
        :param      namespace | <str>
        """
        self._namespace = namespace

    def setIndexes(self, indexes):
        """
        Sets the list of indexed lookups for this schema to the inputed list.
        
        :param      indexes     | [<orb.Index>, ..]
        """
        self._indexes = indexes[:]

    def setInherits(self, name):
        """
        Sets the name for the inherited table schema to the inputed name.
        
        :param      name    | <str>
        """
        self._inherits = name

    def setName(self, name):
        """
        Sets the name of this schema object to the inputed name.
        
        :param      name    | <str>
        """
        self._name = name

    def setGroup(self, group):
        """
        Sets the group association for this schema to the inputed group.
        
        :param      group | <orb.TableGroup>
        """
        self._group = group
        self._groupName = group.name() if group else ''

    def setGroupName(self, groupName):
        """
        Sets the group name that this table schema will be apart of.
        
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
        Sets whether or not to preload all records for this table as
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

    def setStringFormat(self, text):
        """
        Sets a string format to be used when rendering a table using the str()
        method.  This is a python string format with dictionary keys for the 
        column values that you want to display.
        
        :param      format | <str>
        """
        self._stringFormat = nstr(text)

    def setTableName(self, tableName):
        """
        Sets the name that will be used in the actual database.  If the \
        name supplied is blank, then the default database name will be \
        used based on the group and name for this schema.
        
        :param      tableName  | <str>
        """
        self._tableName = tableName

    def setTimezone(self, timezone):
        """
        Sets the timezone associated directly to this database.
        
        :sa     <orb.Orb.setTimezone>
        
        :param     timezone | <pytz.tzfile> || None
        """
        self._timezone = timezone

    def setUseAdvancedFormatting(self, state):
        """
        Sets whether or not to use advanced string formatting for this
        table's string format.
        
        :param      state | <bool>
        """
        self._useAdvancedFormatting = state

    def stringFormat(self):
        """
        Returns the string format style for this schema.
        
        :return     <str>
        """
        return self._stringFormat

    def tableName(self):
        """
        Returns the name that will be used for the table in the database.
        
        :return     <str>
        """
        if not self._tableName:
            grp = self.group()
            prefix = grp.modelPrefix() if grp and grp.useModelPrefix() else ''
            self._tableName = self.defaultTableName(self.name(), prefix)
        return self._tableName

    def timezone(self):
        """
        Returns the timezone associated specifically with this database.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the Orb system in general.
        
        :sa     <orb.Orb>
        
        :return     <pytz.tzfile> || None
        """
        if self._timezone is None:
            return self.database().timezone()
        return self._timezone

    def toolTip(self, context='normal'):
        tip = '''\
<b>{name} <small>(Table from {group} group)</small></b><br>
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

>>> # database table info
>>> schema.databaseName()
'{db_name}'
</pre>'''
        return tip.format(name=self.name(),
                          group=self.groupName(),
                          db_name=self.databaseName(),
                          display=self.displayName())

    def useAdvancedFormatting(self):
        """
        Returns whether or not to use advanced string formatting vs.
        the traditional system (%).
        """
        return self._useAdvancedFormatting

    def toXml(self, xparent):
        """
        Saves this schema information to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xschema = ElementTree.SubElement(xparent, 'schema')

        # save the properties
        xschema.set('name', self.name())
        xschema.set('displayName', self.displayName())
        xschema.set('group', self.groupName())
        xschema.set('inherits', self.inherits())
        xschema.set('dbname', self.tableName())
        xschema.set('autoPrimary', nstr(self.autoPrimary()))
        xschema.set('stringFormat', self.stringFormat())
        xschema.set('cacheEnabled', nstr(self.isCacheEnabled()))
        xschema.set('cacheExpire', nstr(self._cacheExpireIn))
        xschema.set('preloadCache', nstr(self.preloadCache()))
        xschema.set('useAdvanced', nstr(self.useAdvancedFormatting()))

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

    @staticmethod
    def defaultTableName(name, prefix=''):
        """
        Returns the default database table name for the inputed name \
        and prefix.
        
        :param      name    | <str>
        :param      prefix  | <str>
        """
        if prefix:
            templ = TableSchema.TEMPLATE_PREFIXED
        else:
            templ = TableSchema.TEMPLATE_TABLE

        options = {'name': name, 'prefix': prefix}

        return projex.text.render(templ, options)

    @staticmethod
    def fromXml(xschema, referenced=False):
        """
        Generates a new table schema instance for the inputed database schema \
        based on the given xml information.
        
        :param      xschema      | <xml.etree.Element>
        
        :return     <TableSchema> || None
        """
        tschema = orb.TableSchema(referenced=referenced)

        # load the properties
        tschema.setName(xschema.get('name', ''))
        tschema.setDisplayName(xschema.get('displayName', ''))
        tschema.setGroupName(xschema.get('group', ''))
        tschema.setInherits(xschema.get('inherits', ''))
        tschema.setTableName(xschema.get('dbname', ''))
        tschema.setAutoPrimary(xschema.get('autoPrimary') != 'False')
        tschema.setStringFormat(xschema.get('stringFormat', ''))
        tschema.setUseAdvancedFormatting(xschema.get('useAdvanced') == 'True')
        tschema.setCacheEnabled(xschema.get('cacheEnabled') == 'True')
        tschema.setCacheExpireIn(int(xschema.get('cacheExpire', 0)))
        tschema.setPreloadCache(xschema.get('preloadCache') == 'True')

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

