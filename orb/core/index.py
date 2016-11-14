""" Defines an indexing system to use when looking up records. """

import demandimport
import logging
import inflection

from ..utils.enum import enum

with demandimport.enabled():
    import orb


log = logging.getLogger(__name__)


class Index(object):
    """ 
    Defines an indexed way to lookup information from a database.
    Creating an Index generates an object that works like a method, however
    has a preset query built into it, along with caching options.
    """

    Flags = enum(
        'Unique',
        'Private',
        'Static',
        'Virtual'
    )

    def __init__(self,
                 columns=None,
                 name='',
                 dbname='',
                 flags=0,
                 order=None,
                 schema=None):
        self.__name = self.__name__ = name
        self.__dbname = dbname
        self.__columns = columns or []
        self.__flags = self.Flags.from_set(flags) if isinstance(flags, set) else flags
        self.__order = order
        self.__schema = None

        # register this index to the schema (if provided)
        if schema:
            schema.register(self)

    def __call__(self, model, *values, **context):
        """
        Calls the index as a function.  This will generate a query
        for the given model based on the index's columns and the given
        values.  If this index is flagged as `Unique` then the response
        will be an instance of <model> or None, otherwise it will be an
        `orb.Collection` instance.

        :param model: subclass of <orb.Model>
        :param values: tuple of arguments that correspond to the columns of this index
        :param context: <orb.Context> descriptor

        :return: <orb.Collection> or <orb.Model> or None
        """
        context['where'] = self.build_query(values, schema=model.schema()) & context.get('where')

        if self.__order:
            context.setdefault('order', self.__order)

        records = model.select(**context)
        return records.first() if self.test_flag(self.Flags.Unique) else records

    def __eq__(self, other):
        return self is other

    def __json__(self):
        output = {
            'name': self.__name,
            'dbname': self.__dbname,
            'columns': self.__columns,
            'flags': {k: True for k in self.Flags.to_set(self.__flags)},
            'order': self.__order
        }
        return output

    def __lt__(self, other):
        if isinstance(other, Index):
            return self.name() < other.name()
        else:
            return True

    def __ne__(self, other):
        return self is not other

    def build_query(self, values, schema=None):
        """
        Builds the query for this index for the given values.

        :param values: (<variant>, ..)

        :return: <orb.Query>
        """
        columns = self.schema_columns(schema=schema)
        column_count = len(columns)
        value_count = len(values)

        # ensure we have a proper number of columns
        if column_count != value_count:
            opts = (self.__name, column_count, value_count)
            raise TypeError('{0}() takes exactly {1} arguments ({2} given)'.format(*opts))
        else:
            where = orb.Query()
            for i in xrange(column_count):
                where &= orb.Query(columns[i]) == values[i]
            return where

    def copy(self):
        """
        Creates a copy of this index and returns it.

        :return: <orb.Index>
        """
        other = type(self)(
            columns=self.__columns[:],
            name=self.__name,
            dbname=self.__dbname,
            flags=self.__flags,
            order=self.__order
        )
        return other

    def columns(self):
        """
        Returns a list of the columns that are associated with this index.
        
        :return: [<str>, ..]
        """
        return self.__columns

    def dbname(self):
        """
        Returns the database name that will be used for the backend.

        :return: <str>
        """
        if not self.__dbname and self.__schema:
            schema = inflection.underscore(self.__schema.dbname())
            name = inflection.underscore(self.__name)
            return '{0}_{1}_idx'.format(schema, name)
        else:
            return self.__dbname

    def flags(self):
        """
        Returns the flags that are set for this index.

        :return: <orb.Index.Flags>
        """
        return self.__flags

    def name(self):
        """
        Returns the name of this index.
        
        :return: <str>
        """
        return self.__name

    def order(self):
        """
        Returns the default order for this index.

        :return: <str> or [(<str> column, ASC or DESC), ..]
        """
        return self.__order

    def schema(self):
        """
        Returns the associated schema from this index object.

        :return: <orb.Schema>
        """
        return self.__schema

    def schema_columns(self, schema=None):
        """
        Returns a list of the column instances associated with the
        schema object that is associated with this instance.

        :return: [<orb.Column>, ..]
        """
        schema = schema or self.schema()
        if schema is None:
            raise orb.errors.OrbError('Schema is required for collecting columns for {0} index'.format(self.name()))
        return [schema.column(col) for col in self.__columns]

    def set_columns(self, columns):
        """
        Sets the list of the column names that this index will use when \
        looking of the records.
        
        :param columns: [<str> or <orb.Column>, ..]
        """
        self.__columns = columns

    def set_dbname(self, dbname):
        """
        Assigns the database (backend) name that will be used for this
        index.

        :param dbname: <str>
        """
        self.__dbname = dbname

    def set_flags(self, flags):
        """
        Sets the flags that are stored on this index.

        :param flags: <orb.Index.Flags>
        """
        self.__flags = flags

    def set_name(self, name):
        """
        Sets the name for this index to this index.
        
        :param name: <str>
        """
        self.__name = self.__name__ = name

    def set_order(self, order):
        """
        Sets the order information for this index for how to sort and \
        organize the looked up data.

        :param order: <str> or [(<str> column, ASC or DESC), ..]
        """
        self.__order = order

    def set_schema(self, schema):
        """
        Assigns the schema for this index.  This method will only update the
        pointer from the index to the schema - to properly register an index
        to a schema though, you should use the `orb.Schema.register` method and
        supply this schema index.  That will in turn call this method.

        :param schema: <orb.Schema>
        """
        self.__schema = schema

    def validate(self, values):
        """
        Validates whether or not this index's requirements are satisfied by the inputted record and
        values.  If this index fails validation, an InvalidIndexArguments will be raised.

        :param values: {<orb.Column>: <variant>, ..}

        :return     <bool>
        """
        columns = self.schema_columns()
        try:
            _ = [values[col] for col in columns]
        except orb.errors.ColumnNotFound as err:
            raise orb.errors.InvalidIndexArguments(self.schema(), msg=str(err))
        except KeyError as err:
            msg = 'Missing {0} from {1}.{2} index'.format(err[0].name(),
                                                          self.schema().name(),
                                                          self.name())
            raise orb.errors.InvalidIndexArguments(self.schema(), msg=msg)
        else:
            return True

    def test_flag(self, flags):
        """
        Tests to see if the given flags are set against this
        index's flags.

        :param flags: <orb.Index.Flags>
        """
        return self.Flags.test_flag(self.__flags, flags)
