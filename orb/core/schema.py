""" Defines the meta information for a Table class. """

import logging

from projex.lazymodule import lazy_import

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Schema(object):
    """ 
    Contains meta data information about a table as it maps to a database.
    """

    def __init__(self,
                 name,
                 dbname='',
                 display='',
                 inherits='',
                 idColumn='id',
                 abstract=False,
                 archived=False,
                 columns=None,
                 indexes=None,
                 pipes=None,
                 views=None,
                 database=''):
        self.__name = name
        self.__abstract = abstract
        self.__dbname = dbname or orb.system.syntax().schemadb(name)
        self.__database = database
        self.__inherits = inherits
        self.__display = display
        self.__archived = archived
        self.__idColumn = idColumn

        self.__model = None
        self.__archiveModel = None

        self.__columns = columns or {}
        self.__indexes = indexes or {}
        self.__pipes = pipes or {}
        self.__views = views or {}

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

    def addPipe(self, pipe):
        """
        Adds the inputted pipe reference to this table schema.

        :param      pipe | <orb.Pipe>
        """
        pipe.setSchema(self)
        self.__pipes[pipe.name()] = pipe

    def archiveModel(self):
        return self.__archiveModel

    def column(self, col, recurse=True, flags=0):
        """
        Returns the column instance based on its name.  
        If error reporting is on, then the ColumnNotFound
        error will be thrown the key inputted is not a valid
        column name.
        
        :param      name | <str>
                    recurse | <bool>
                    flags | <int>

        :return     <orb.Column> || None
        """
        if isinstance(col, orb.Column):
            return col

        parts = col.split('.')
        key = parts[0]
        found = None
        for column in self.columns(recurse=recurse, flags=flags).values():
            if key in (column.name(), column.field()):
                found = column
                break

        if found and len(parts) > 1:
            ref = found.referenceModel()
            if ref:
                found = ref.schema().column('.'.join(parts[1:]), recurse=recurse, flags=flags)
            else:
                found = None

        return found

    def columns(self, recurse=True, flags=0):
        """
        Returns the list of column instances that are defined
        for this table schema instance.
        
        :param      recurse | <bool>
                    flags   | <orb.Column.Flags>
                    kind    | <orb.Column.Kind>
        
        :return     {<str> column name: <orb.Column>, ..}
        """
        output = {col.name(): col for col in self.__columns.values() if (not flags or col.testFlag(flags))}

        if recurse:
            inherits = self.inherits()
            if inherits:
                schema = orb.system.schema(inherits)
                if not schema:
                    raise orb.errors.ModelNotFound(inherits)
                else:
                    ancest_columns = schema.columns(recurse=recurse, flags=flags)
                    dups = set(ancest_columns.keys()).intersection(output.keys())
                    if dups:
                        raise orb.error.DuplicateColumnFound(self.name(), ','.join(dups))
                    else:
                        output.update(ancest_columns)

        return output

    def database(self):
        return self.__database

    def display(self):
        """
        Returns the display name for this table.
        
        :return     <str>
        """
        return self.__display or orb.system.syntax().display(self.__name)

    def hasColumn(self, column, recurse=True, flags=0):
        """
        Returns whether or not this column exists within the list of columns
        for this schema.
        
        :return     <bool>
        """
        return column in self.columns(recurse=recurse, flags=flags)

    def hasTranslations(self):
        for col in self.columns().values():
            if col.testFlag(col.Flags.Translatable):
                return True
        return False

    def idColumn(self):
        return self.__idColumn

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
                raise orb.errors.ModelNotFound(self.inherits())
            else:
                output.update(schema.indexes(recurse=recurse))
        return output

    def inherits(self):
        """
        Returns the name of the table schema that this class will inherit from.
        
        :return     <str>
        """
        return self.__inherits

    def isAbstract(self):
        """
        Returns whether or not this schema is an abstract table.  Abstract \
        tables will not register to the database, but will serve as base \
        classes for inherited tables.
        
        :return     <bool>
        """
        return self.__abstract

    def isArchived(self):
        """
        Returns whether or not this schema is archived.  Archived schema's will store additional records
        each time a record is created or updated for historical reference.

        :return     <bool>
        """
        return self.__archived

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

    def pipe(self, name, recurse=True):
        """
        Returns the pipe that matches the inputted name.

        :return     <orb.Pipe> || None
        """
        return self.pipes(recurse=recurse).get(name)

    def pipes(self, recurse=True):
        """
        Returns a list of the pipes for this instance.
        
        :return     [<orb.Pipe>, ..]
        """
        output = self.__pipes.copy()
        if recurse and self.inherits():
            schema = orb.system.schema(self.inherits())
            if not schema:
                raise orb.errors.ModelNotFound(self.inherits())
            else:
                output.update(schema.pipes(recurse=recurse))
        return output

    def reverseLookup(self, name):
        """
        Returns the reverse lookup that matches the inputted name.

        :return     <orb.Column> || None
        """
        return self.reverseLookups().get(name)

    def reverseLookups(self):
        """
        Returns a list of all the reverse-lookup columns that reference this schema.

        :return     [<orb.Column>, ..]
        """
        return {col.reverseInfo().name: col for schema in orb.system.schemas().values()
                for col in schema.columns().values()
                if isinstance(col, orb.ReferenceColumn) and col.reference() == self.name() and col.reverseInfo()}

    def setAbstract(self, state):
        """
        Sets whether or not this table is abstract.
        
        :param      state | <bool>
        """
        self.__abstract = state

    def setArchived(self, state=True):
        """
        Sets the archive state for this schema.

        :param      state | <bool>
        """
        self.__archived = state

    def setArchiveModel(self, model):
        self.__archiveModel = model

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

    def setIdColumn(self, column):
        self.__idColumn = column

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

    def setPipes(self, pipes):
        """
        Sets the pipe methods that will be used for this schema.
        
        :param      pipes | [<orb.Pipes>, ..]
        """
        self.__pipes = {}
        for name, pipe in pipes.items():
            self.__pipes[name] = pipe
            pipe.setSchema(self)

    def setDbName(self, dbname):
        """
        Sets the name that will be used in the actual database.  If the \
        name supplied is blank, then the default database name will be \
        used based on the group and name for this schema.
        
        :param      dbname  | <str>
        """
        self.__dbname = dbname

    def setViews(self, views):
        """
        Adds a new view to this schema.  Views provide pre-built dynamically joined tables that can
        give additional information to a table.

        :param      name | <str>
                    view | <orb.View>
        """
        self.__views = {}
        for view in views:
            self.__views[view.name()] = view

    def dbname(self):
        """
        Returns the name that will be used for the table in the database.
        
        :return     <str>
        """
        return self.__dbname

    def views(self, recurse=True):
        """
        Returns the view for this schema that matches the given name.

        :return     <orb.View> || None
        """
        output = self.__views.copy()
        if recurse and self.inherits():
            schema = orb.system.schema(self.inherits())
            if not schema:
                raise orb.errors.ModelNotFound(self.inherits())
            else:
                output.update(schema.views(recurse=recurse))
        return output

