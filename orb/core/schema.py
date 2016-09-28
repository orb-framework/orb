""" Defines the meta information for a Table class. """

import logging
import inflection

from collections import OrderedDict as odict
from projex.enum import enum
from projex.lazymodule import lazy_import

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Schema(object):
    """ 
    Contains meta data information about a table as it maps to a database.
    """
    Flags = enum('Abstract', 'Static')

    def __json__(self):
        # make sure we're only exposing desired public data
        columns = []
        collectors = []
        for col in self.columns().values():
            if not col.testFlag(col.Flags.Private):
                if not isinstance(col, orb.ReferenceColumn) or getattr(col.referenceModel(), '__resource__', False):
                    columns.append(col.__json__())

        for coll in self.collectors().values():
            if not coll.testFlag(coll.Flags.Private):
                if not coll.model() or getattr(coll.model(), '__resource__', False):
                    collectors.append(coll.__json__())

        indexes = [index.__json__() for index in self.indexes().values() if not index.testFlag(index.Flags.Private)]

        output = {
            'model': self.name(),
            'idColumn': self.idColumn().field(),
            'dbname': self.dbname(),
            'display': self.display(),
            'inherits': self.inherits(),
            'flags': {k: True for k in self.Flags.toSet(self.__flags)},
            'columns': {col['field']: col for col in columns},
            'indexes': {index['name']: index for index in indexes},
            'collectors': {coll['name']: coll for coll in collectors}
        }
        return output

    def __init__(self,
                 name,
                 dbname='',
                 display='',
                 inherits='',
                 database='',
                 namespace='',
                 idColumn='id',
                 flags=0,
                 columns=None,
                 indexes=None,
                 collectors=None):

        default_db_name = inflection.pluralize(inflection.underscore(name))
        self.__name = name
        self.__dbname = dbname or default_db_name
        self.__database = database
        self.__namespace = namespace
        self.__flags = flags
        self.__inherits = inherits
        self.__display = display
        self.__cache = {}
        self.__idColumn = idColumn

        self.__model = None
        self.__columns = columns or {}
        self.__indexes = indexes or {}
        self.__collectors = collectors or {}

    def __cmp__(self, other):
        # check to see if this is the same instance
        if id(self) == id(other):
            return 0

        # make sure this instance is a valid one for the other kind
        if not isinstance(other, Schema):
            return -1

        # compare inheritance level
        my_ancestry = self.ancestry()
        other_ancestry = other.ancestry()

        result = cmp(len(my_ancestry), len(other_ancestry))
        if not result:
            return cmp(self.name(), other.name())
        return result

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
        if not self.inherits():
            return []

        schema = orb.system.schema(self.inherits())
        if not schema:
            return []

        return schema.ancestry() + [schema]

    def addColumn(self, column):
        """
        Adds the inputted column to this table schema.

        :param      column  | <orb.Column>
        """
        column.setSchema(self)
        self.__columns[column.name()] = column

    def addIndex(self, index):
        """
        Adds the inputted index to this table schema.

        :param      index   | <orb.Index>
        """
        index.setSchema(self)
        self.__indexes[index.name()] = index

    def addCollector(self, collector):
        """
        Adds the inputted collector reference to this table schema.

        :param      collector | <orb.Collector>
        """
        collector.setSchema(self)
        self.__collectors[collector.name()] = collector

    def collector(self, name, recurse=True):
        """
        Returns the collector that matches the inputted name.

        :return     <orb.Collector> || None
        """
        return self.collectors(recurse=recurse).get(name)

    def collectors(self, recurse=True, flags=0):
        """
        Returns a list of the collectors for this instance.

        :return     {<str> name: <orb.Collector>, ..}
        """
        output = {}
        if recurse and self.inherits():
            schema = orb.system.schema(self.inherits())
            if not schema:
                raise orb.errors.ModelNotFound(schema=self.inherits())
            else:
                iflags = (flags & ~orb.Collector.Flags.Virtual) if flags else ~orb.Collector.Flags.Virtual
                output.update(schema.collectors(recurse=recurse, flags=iflags))
        output.update({c.name(): c for c in self.__collectors.values() if not flags or c.testFlag(flags)})
        return output

    def column(self, key, recurse=True, flags=0, raise_=True):
        """
        Returns the column instance based on its name.  
        If error reporting is on, then the ColumnNotFound
        error will be thrown the key inputted is not a valid
        column name.
        
        :param      key     | <str> || <orb.Column> || <list>
                    recurse | <bool>
                    flags   | <int>

        :return     <orb.Column> || None
        """
        if isinstance(key, orb.Column):
            return key
        else:
            cache_key = (key, recurse, flags)
            try:
                last_column = self.__cache[cache_key]
            except KeyError:

                parts = key.split('.')
                schema = self
                last_column = None

                for part in parts:
                    cols = schema.columns(recurse=recurse, flags=flags)
                    found = None

                    for column in cols.values():
                        if part in (column.name(), column.field()):
                            found = column
                            break

                    if found is None:
                        break

                    elif isinstance(found, orb.ReferenceColumn):
                        schema = found.referenceModel().schema()

                    last_column = found

                self.__cache[cache_key] = last_column

            # return the column response
            if last_column is not None:
                return last_column
            elif raise_:
                raise orb.errors.ColumnNotFound(schema=self, column=key)
            else:
                return None

    def columns(self, recurse=True, flags=0):
        """
        Returns the list of column instances that are defined
        for this table schema instance.
        
        :param      recurse | <bool>
                    flags   | <orb.Column.Flags>
                    kind    | <orb.Column.Kind>
        
        :return     {<str> column name: <orb.Column>, ..}
        """
        key = (recurse, flags)
        try:
            return self.__cache[key]
        except KeyError:

            output = odict()

            if recurse:
                inherits = self.inherits()
                if inherits:
                    schema = orb.system.schema(inherits)
                    if not schema:
                        raise orb.errors.ModelNotFound(schema=inherits)
                    else:
                        # lookup ancestor columns (don't care about virtual columns)
                        iflags = (flags & ~orb.Column.Flags.Virtual) if flags else ~orb.Column.Flags.Virtual
                        ancest_columns = schema.columns(recurse=recurse, flags=iflags)
                        dups = set(ancest_columns.keys()).intersection(output.keys())
                        if dups:
                            raise orb.errors.DuplicateColumnFound(schema=self, column=','.join(dups))
                        else:
                            output.update(ancest_columns)

            output.update(odict(
                (col.name(), col) for col
                in sorted(self.__columns.values(), key=lambda x: x.order())
                if (not flags or col.testFlag(flags))
            ))

            self.__cache[key] = output
            return output

    def database(self):
        return self.__database

    def dbname(self):
        """
        Returns the name that will be used for the table in the database.

        :return     <str>
        """
        return self.__dbname

    def display(self):
        """
        Returns the display name for this table.
        
        :return     <str>
        """
        return self.__display or inflection.titleize(self.__name)

    def hasColumn(self, column, recurse=True, flags=0):
        """
        Returns whether or not this column exists within the list of columns
        for this schema.
        
        :return     <bool>
        """
        return column in self.columns(recurse=recurse, flags=flags)

    def hasTranslations(self):
        for col in self.columns().values():
            if col.testFlag(col.Flags.I18n):
                return True
        return False

    def idColumn(self):
        return self.column(self.__idColumn)

    def index(self, name, recurse=True):
        return self.indexes(recurse=recurse).get(name)

    def indexes(self, recurse=True):
        """
        Returns the list of indexes that are associated with this schema.
        
        :return     [<orb.Index>, ..]
        """
        output = self.__indexes.copy()
        if recurse and self.inherits():
            schema = orb.system.schema(self.inherits())
            if not schema:
                raise orb.errors.ModelNotFound(schema=self.inherits())
            else:
                output.update(schema.indexes(recurse=recurse))
        return output

    def inherits(self):
        """
        Returns the name of the table schema that this class will inherit from.
        
        :return     <str>
        """
        return self.__inherits

    def inheritanceTree(self):
        """
        Returns the inheritance tree for this schema, traversing up the hierarchy for the inherited schema instances.

        :return: <generator>
        """
        inherits = self.inherits()
        while inherits:
            ischema = orb.system.schema(inherits)
            if not ischema:
                raise orb.errors.ModelNotFound(schema=inherits)

            yield ischema
            inherits = ischema.inherits()

    def model(self, autoGenerate=False):
        """
        Returns the default Table class that is associated with this \
        schema instance.
        
        :param      autoGenerate | <bool>
        
        :return     <subclass of Table>
        """
        if self.__model is None and autoGenerate:
            self.__model = orb.system.generateModel(self)
            self.setModel(self.__model)
        return self.__model

    def name(self):
        """
        Returns the name of this schema object.
        
        :return     <str>
        """
        return self.__name

    def namespace(self, **context):
        """
        Returns the namespace that should be used for this schema, when specified.

        :return: <str>
        """
        context = orb.Context(**context)
        if context.forceNamespace:
            return context.namespace or self.__namespace
        else:
            return self.__namespace or context.namespace

    def register(self, item):
        """
        Registers a new orb object to this schema.  This could be a column, index, or collector -- including
        a virtual object defined through the orb.virtual decorator.

        :param item: <variant>

        :return:
        """
        if callable(item) and hasattr(item, '__orb__'):
            item = item.__orb__

        key = item.name()
        model = self.__model

        # create class methods for indexes
        if isinstance(item, orb.Index):
            self.__indexes[key] = item
            item.setSchema(self)

            if model and not hasattr(model, key):
                setattr(model, key, classmethod(item))

        # create instance methods for collectors
        elif isinstance(item, orb.Collector):
            self.__collectors[key] = item
            item.setSchema(self)

        # create instance methods for columns
        elif isinstance(item, orb.Column):
            self.__columns[key] = item
            item.setSchema(self)

    def setColumns(self, columns):
        """
        Sets the columns that this schema uses.
        
        :param      columns     | [<orb.Column>, ..]
        """
        self.__columns = {}
        for name, column in columns.items():
            self.__columns[name] = column
            column.setSchema(self)

    def setDisplay(self, name):
        """
        Sets the display name for this table.
        
        :param      name | <str>
        """
        self.__display = name

    def setModel(self, model):
        """
        Sets the default Table class that is associated with this \
        schema instance.
        
        :param    model     | <subclass of Table>
        """
        self.__model = model

    def setIndexes(self, indexes):
        """
        Sets the list of indexed lookups for this schema to the inputted list.
        
        :param      indexes     | [<orb.Index>, ..]
        """
        self.__indexes = {}
        for name, index in indexes.items():
            self.__indexes[name] = index
            index.setSchema(self)

    def setInherits(self, name):
        """
        Sets the name for the inherited table schema to the inputted name.
        
        :param      name    | <str>
        """
        self.__inherits = name

    def setName(self, name):
        """
        Sets the name of this schema object to the inputted name.
        
        :param      name    | <str>
        """
        self.__name = name

    def setNamespace(self, namespace):
        """
        Sets the namespace for this schema object to the given value.  This is a way to differentiate the same
        model from different locations within the backend.

        :param namespace: <str>
        """
        self.__namespace = namespace

    def setCollectors(self, collectors):
        """
        Sets the collector methods that will be used for this schema.
        
        :param      collectors | [<orb.Collectors>, ..]
        """
        self.__collectors = {}
        for name, collector in collectors.items():
            self.__collectors[name] = collector
            collector.setSchema(self)

    def setDbName(self, dbname):
        """
        Sets the name that will be used in the actual database.  If the \
        name supplied is blank, then the default database name will be \
        used based on the group and name for this schema.
        
        :param      dbname  | <str>
        """
        self.__dbname = dbname

    def testFlags(self, flags):
        return (self.__flags & flags) != 0
