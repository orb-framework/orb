import demandimport
import logging
import inflection

from collections import OrderedDict as odict
from ..utils.enum import enum

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class Schema(object):
    Flags = enum('Abstract', 'Static', 'Private')

    def __init__(self,

                 # class properties
                 name='',
                 display='',
                 group='',
                 inherits='',
                 id_column='id',
                 flags=0,

                 # database properties
                 alias='',
                 dbname='',
                 namespace='',
                 database='',

                 # object properties
                 columns=None,
                 indexes=None,
                 collectors=None,
                 system=None):

        if isinstance(columns, (list, set)):
            columns = {x.name(): x for x in columns}
        if isinstance(indexes, (list, set)):
            indexes = {x.name(): x for x in indexes}
        if isinstance(collectors, (list, set)):
            collectors = {x.name(): x for x in collectors}

        # class properties
        self.__name = name
        self.__display = display
        self.__group = group
        self.__inherits = inherits
        self.__id_column = id_column
        self.__flags = Schema.Flags.from_set(flags) if isinstance(flags, set) else flags

        # database properties
        self.__alias = alias
        self.__dbname = dbname
        self.__namespace = namespace
        self.__database = database

        # object properties
        self.__cache = {}
        self.__model = None
        self.__system = system or orb.system
        self.__objects = {
            'columns': columns or {},
            'indexes': indexes or {},
            'collectors': collectors or {}
        }

        # ensure the given objects have the schema assigned
        if columns:
            for column in columns.values():
                column.set_schema(self)
        if indexes:
            for index in indexes.values():
                index.set_schema(self)
        if collectors:
            for collector in collectors.values():
                collector.set_schema(self)

    def __cmp__(self, other):
        # check to see if this is the same instance
        if self is other:
            return 0

        # make sure this instance is a valid one for the other kind
        elif not isinstance(other, Schema):
            return -1

        # compare inheritance level
        else:
            my_depth = len(list(self.ancestry()))
            other_depth = len(list(other.ancestry()))

            if my_depth == other_depth:
                return cmp(self.name(), other.name())
            else:
                return cmp(my_depth, other_depth)

    def __json__(self):
        """
        Serializes the schema object as a dictionary to be able to be shared with
        JSON.

        :return: <dict>
        """
        # serialize columns
        columns = []
        for col in self.columns().values():
            # do not serialize private columns (only available to Python APIs)
            if col.test_flag(col.Flags.Private):
                continue

            # do not serialize columns that refer to non-resources
            elif (isinstance(col, orb.ReferenceColumn) and
                  col.reference_model().schema().test_flag(Schema.Flags.Private)):
                continue

            # serialize the column otherwise
            else:
                columns.append(col.__json__())

        # serialize collectors
        collectors = []
        for coll in self.collectors().values():
            # do not serialize private collectors (only available to Python APIs)
            if coll.test_flag(coll.Flags.Private):
                continue

            # do not serialize collectors associated to non-resources
            elif coll.model() and coll.model().schema().test_flag(Schema.Flags.Private):
                continue

            # serialize the collector otherwise
            else:
                collectors.append(coll.__json__())

        # serialize indexes
        indexes = []
        for index in self.indexes().values():
            # do not serialize private indexes (only available to Python APIs)
            if index.test_flag(index.Flags.Private):
                continue

            # serialize the index otherwise
            else:
                indexes.append(index.__json__())

        # return the schema definition
        output = {
            'model': self.name(),
            'id_column': self.id_column().alias(),
            'dbname': self.alias(),
            'display': self.display(),
            'inherits': self.inherits(),
            'flags': {k: True for k in self.Flags.to_set(self.__flags)},
            'columns': {col['field']: col for col in columns},
            'indexes': {index['name']: index for index in indexes},
            'collectors': {coll['name']: coll for coll in collectors}
        }
        return output

    def _collect(self, key, flags_type, recurse=True, flags=0):
        """
        Returns a collection of objects that are associated with this schema.
        Set the `recurse` flag to False to yield only collectors directly associated
        with this schema vs. collectors from the full hierarchy.  Use the `flags`
        keyword to filter based on collector property flags.

        :param key: <str>
        :param flags_type: <enum>
        :param recurse: <bool>
        :param flags: <ints>

        :return: {<str> name: <orb.Collector> or <orb.Column> or <orb.Index>, ..}
        """
        cache_key = (key, recurse, flags)
        try:
            return self.__cache[cache_key]
        except KeyError:
            schemas = [self]

            # generate the recursive lookup of schemas
            if recurse:
                schema = self
                while schema.inherits():
                    schema = schema.system().schema(schema.inherits())
                    schemas.append(schema)

            # update the output dictionary
            output = odict()
            for schema in reversed(schemas):
                output.update(odict(
                    (obj.name(), obj)
                    for obj in sorted(schema.__objects[key].values())
                    if not flags or obj.test_flag(flags)
                ))

            self.__cache[cache_key] = output
            return output

    def alias(self):
        """
        Returns the alias that will be returned from the backend for this schema.  By default
        this property will be the same as the raw `dbname`, but can be overridden to separate
        backend / database naming from the API naming.

        :return: <str>
        """
        return self.__alias or self.dbname()

    def ancestry(self):
        """
        Iterates over the inheritance hierarchy for this schema.  The response from this
        method will be a generator that yields each inherited level.

        :return: <generator>
        """
        schema = self
        while schema.inherits():
            schema = schema.system().schema(schema.inherits())
            yield schema

    def collector(self, name, recurse=True, flags=0):
        """
        Returns the collector by name for this schema.  If the recurse
        flag is True, then all inherited collectors will be included
        in the lookup.

        :return: <orb.Collector> or None
        """
        return self.collectors(recurse=recurse, flags=flags).get(name)

    def collectors(self, recurse=True, flags=0):
        """
        Returns a collection of collectors that are associated with this schema.
        Set the `recurse` flag to False to yield only collectors directly associated
        with this schema vs. collectors from the full hierarchy.  Use the `flags`
        keyword to filter based on collector property flags.

        :param recurse: <bool>
        :param flags: <orb.Collector.Flags>

        :return: {<str> name: <orb.Collector>, ..}
        """
        return self._collect('collectors', orb.Collector.Flags, recurse=recurse, flags=flags)

    def column(self, key, recurse=True, flags=0, raise_=True):
        """
        Returns the column associated with the given key.  This
        method can be used to normalize column input from strings
        to other column instances.

        This method also supports column traversal using the dot-noted
        column names.

        If the `raise_` flag is set to True, then if the column is not found,
        the method is raise a ColumnNotFound error.

        :usage:

            schema.column('id')
            schema.column('user.group.name')
            schema.column(orb.StringColumn(name='username'))

        :param key: <str> or <orb.Column>
        :param recurse: <bool>
        :param flags: <int>
        :param raise_: <bool>

        :return     <orb.Column> or None
        """
        # lookup a specific column
        if isinstance(key, orb.Column):
            if key.schema() is self:
                return key
            else:
                key = key.name()

        cache_key = (key, recurse, flags)

        # lookup based on the cache
        if cache_key in self.__cache:
            output = self.__cache[cache_key]
            if output is None and raise_:
                raise orb.errors.ColumnNotFound(schema=self, column=key)
            else:
                return output


        # lookup a traversal
        elif '.' in key:
            parts = key.split('.')
            schema = self

            for i, part in enumerate(parts):
                col = schema.column(part, recurse=recurse, flags=flags, raise_=False)

                # if this is the last of the traversal, return the column
                if i == len(parts) - 1:
                    self.__cache[cache_key] = col
                    if raise_ and col is None:
                        raise orb.errors.ColumnNotFound(schema=schema, column=part)
                    else:
                        return col

                # otherwise, if this column is a reference column then
                # lookup the next part
                elif isinstance(col, orb.ReferenceColumn):
                    schema = col.reference_model().schema()

                # otherwise, raise the error that it could not be found
                elif raise_:
                    self.__cache[cache_key] = None
                    raise orb.errors.ColumnNotFound(schema=self, column=key)

                else:
                    self.__cache[cache_key] = None
                    return None

        # return the column based on the standard lookup
        else:
            cols = self.columns(recurse=recurse, flags=flags)
            print self.__model, key, cols.keys()
            try:
                output = cols[key]
            except KeyError:
                for col in cols.values():
                    if key in (col.name(), col.alias(), col.field()):
                        output = col
                        break
                else:
                    output = None

            self.__cache[cache_key] = output
            if output is None and raise_:
                raise orb.errors.ColumnNotFound(schema=self, column=key)
            else:
                return output

    def columns(self, recurse=True, flags=0):
        """
        Returns the list of column instances that are defined
        for this table schema instance.

        :param      recurse | <bool>
                    flags   | <orb.Column.Flags>
                    kind    | <orb.Column.Kind>

        :return     {<str> column name: <orb.Column>, ..}
        """
        return self._collect('columns', orb.Column.Flags, recurse=recurse, flags=flags)

    def database(self):
        """
        Returns the name of the database that this schema is associated with.

        :return: <str>
        """
        return self.__database

    def dbname(self):
        """
        Returns the name that will be used for the table in the database.

        :return     <str>
        """
        return self.__dbname or inflection.tableize(self.name())

    def display(self):
        """
        Returns the display name for this table.

        :return     <str>
        """
        return self.__display or inflection.titleize(self.__name)

    def flags(self):
        """
        Returns the flags that are set on this schema object.

        :return: <int>
        """
        return self.__flags

    def generate_model(self):
        """
        Generates a new dynamic model class type based on this schema.

        :return: subclass of <orb.Model>
        """
        args = {
            '__register__': False,
            '__schema__': self
        }
        new_model = type(self.name(), (orb.Table,), args)
        return new_model

    def group(self):
        """
        Returns the group name association this schema has.

        :return: <str>
        """
        return self.__group

    def has_column(self, column, recurse=True, flags=0):
        """
        Returns whether or not this column exists within the list of columns
        for this schema.

        :param column: <orb.Column> or <str>

        :return: <bool>
        """
        if isinstance(column, orb.Column):
            return column in self.columns(recurse=recurse, flags=flags).values()
        else:
            return self.column(column, recurse=recurse, flags=flags, raise_=False) is not None

    def has_translations(self):
        """
        Returns whether or not this schema has internationalization columns
        associated with it.

        :return: <bool>
        """
        for col in self.columns().values():
            if col.test_flag(col.Flags.I18n):
                return True
        return False

    def id_column(self):
        """
        Returns the column that is being used as the `id` column for a model.
        By default, this will be a column with the name `'id'`, but can be
        specified when the model is created as any name.

        :return: <orb.Column>
        """
        return self.column(self.__id_column)

    def index(self, name, recurse=True, flags=0):
        """
        Returns the index based on the given name for this schema.

        :param name: <str>
        :param recurse: <bool>
        :param flags: <int>

        :return: <orb.Index> or None
        """
        return self.indexes(recurse=recurse).get(name)

    def indexes(self, recurse=True, flags=0):
        """
        Returns a collection of indexes associated with this schema.

        :return: {<str> name: <orb.Index>, ..}
        """
        return self._collect('indexes', orb.Index.Flags, recurse=recurse, flags=flags)

    def inherits(self):
        """
        Returns the schema name that this schema inherits from, if any.

        :return: <str>
        """
        return self.__inherits

    def model(self, auto_generate=False):
        """
        Returns the `orb.Model` class object that is the representation of
        this schema as a Python class.  If the `auto_generate` flag is set to True,
        then the model will be generated based on the schema properties, if it has
        not already been set.

        :param auto_generate: <bool>

        :return: subclass of <orb.Model>
        """
        if self.__model is None and auto_generate:
            self.__model = self.generate_model()
        return self.__model

    def name(self):
        """
        Returns the reference name of this schema object.

        :return     <str>
        """
        return self.__name

    def namespace(self, **context):
        """
        Returns the namespace that should be used for this schema, when specified.  There
        are two options when it comes to namespacing within an ORB context - the base
        `namespace` text will be used if no specific namespace is defined on this schema
        object, unless the `force_namespace` context option is set to True - in which case
        the context's namespace will be overridden.

        :return: <str>
        """
        context = orb.Context(**context)
        if context.force_namespace and context.namespace:
            return context.namespace
        elif self.__namespace:
            return self.__namespace
        else:
            return context.namespace

    def register(self, obj):
        """
        Registers a new ORB object to this schema.  This could include an `orb.Column`,
        `orb.Index`, or `orb.Collector` - either as a raw instance, or as a virtual
        object defined using the `orb.virtual` decorator.

        :param obj: <orb.Column> or <orb.Index> or <orb.Collector>

        :return:
        """
        # when modifying the structure, clear out the cache
        self.__cache.clear()

        # look to see if this is a virtual method defining ORB information
        if callable(obj) and hasattr(obj, '__orb__'):
            obj = obj.__orb__

        key = obj.name()
        model = self.__model

        # create class methods for indexes
        if isinstance(obj, orb.Index):
            self.__objects['indexes'][key] = obj
            obj.set_schema(self)

            # register the index class method on the model if it has
            # not been already defined
            if model and not hasattr(model, key):
                setattr(model, key, classmethod(obj))

        # create instance methods for collectors
        elif isinstance(obj, orb.Collector):
            self.__objects['collectors'][key] = obj
            obj.set_schema(self)

        # create instance methods for columns
        elif isinstance(obj, orb.Column):
            self.__objects['columns'][key] = obj
            obj.set_schema(self)

        # raise an invalid error
        else:
            raise RuntimeError('Invalid object reference type: {0}'.format(obj))

    def system(self):
        """
        Returns the underlying system that this schema is associated to.

        :return: <orb.System>
        """
        return self.__system

    def set_model(self, model):
        """
        Sets the model class type that is associated with this schema instance.

        :param model: subclass of <orb.Model> or None
        """
        self.__model = model

    def set_inherits(self, inherits):
        """
        Sets the inheritance schema for this instance.

        :param name: <str>
        """
        self.__inherits = inherits

    def test_flag(self, flags):
        """
        Tests to see if this schema has the given flags set.

        :param flags: <orb.Schema.Flags>

        :return: <bool>
        """
        return self.Flags.test_flag(self.__flags, flags)
