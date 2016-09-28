""" Defines an indexing system to use when looking up records. """

import logging
import inflection

from projex.enum import enum
from projex.lazymodule import lazy_import


log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Index(object):
    """ 
    Defines an indexed way to lookup information from a database.
    Creating an Index generates an object that works like a method, however
    has a preset query built into it, along with caching options.
    """

    Flags = enum(
        'Unique',
        'Private',
        'Static'
    )

    def __json__(self):
        output = {
            'name': self.__name,
            'dbname': self.__dbname,
            'columns': self.__columns,
            'flags': {k: True for k in self.Flags.toSet(self.__flags)},
            'order': self.__order
        }
        return output

    def __init__(self, columns=None, name='', dbname='', flags=0, order=None):
        self.__name = self.__name__ = name
        self.__dbname = dbname
        self.__columns = columns or []
        self.__flags = self.Flags.fromSet(flags) if isinstance(flags, set) else flags
        self.__order = order
        self.__schema = None

    def __call__(self, model, *values, **context):
        # make sure we have the right number of arguments
        if len(values) != len(self.__columns):
            name = self.__name
            columnCount = len(self.__columns)
            valueCount = len(values)
            opts = (name, columnCount, valueCount)
            text = '%s() takes exactly %i arguments (%i given)' % opts
            raise TypeError(text)

        # create the lookup query
        schema = model.schema()
        query = orb.Query()
        for i, col in enumerate(self.__columns):
            value = values[i]
            column = schema.column(col)

            if isinstance(value, orb.Model) and not value.isRecord():
                return None if self.testFlag(self.Flags.Unique) else orb.Collection()
            elif not column:
                raise errors.ColumnNotFound(schema=schema, column=col)

            query &= orb.Query(col) == value

        context['where'] = query & context.get('where')

        records = model.select(**context)
        return records.first() if self.testFlag(self.Flags.Unique) else records

    def copy(self):
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
        Returns the list of column names that this index will be expecting as \
        inputs when it is called.
        
        :return     [<str>, ..]
        """
        schema = self.schema()
        return [schema.column(col) for col in self.__columns]

    def dbname(self):
        schema = inflection.underscore(self.__schema.dbname())
        name = inflection.underscore(self.__name)

        default_db_name = '{0}_{1}_idx'.format(schema, name)
        return self.__dbname or default_db_name

    def flags(self):
        return self.__flags

    def name(self):
        """
        Returns the name of this index.
        
        :return     <str>
        """
        return self.__name

    def schema(self):
        return self.__schema

    def setColumns(self, columns):
        """
        Sets the list of the column names that this index will use when \
        looking of the records.
        
        :param      columns | [<str>, ..]
        """
        self.__columns = columns

    def setOrder(self, order):
        """
        Sets the order information for this index for how to sort and \
        organize the looked up data.
        
        :param      order   | [(<str> field, <str> direction), ..]
        """
        self.__order = order

    def setDbName(self, dbname):
        self.__dbname = dbname

    def setFlags(self, flags):
        self.__flags = flags

    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name = self.__name__ = name

    def setSchema(self, schema):
        self.__schema = schema

    def validate(self, record, values):
        """
        Validates whether or not this index's requirements are satisfied by the inputted record and
        values.  If this index fails validation, a ValidationError will be raised.

        :param      record | subclass of <orb.Table>
                    values | {<orb.Column>: <variant>, ..}

        :return     <bool>
        """
        schema = record.schema()
        columns = self.columns()
        try:
            column_values = [values[col] for col in columns]
        except KeyError as err:
            msg = 'Missing {0} from {1}.{2} index'.format(err[0].name(),
                                                          record.schema().name(),
                                                          self.name())
            raise errors.InvalidIndexArguments(self.schema(), msg=msg)

        # # ensure a unique record is preserved
        # if self.unique():
        #     lookup = getattr(record, self.name())
        #     other = lookup(*column_values)
        #     if other and other != record:
        #         msg = 'A record already exists with the same {0} combination.'.format(', '.join(self.columnNames()))
        #         raise errors.IndexValidationError(self, msg=msg)

        return True

    def testFlag(self, flags):
        return (self.__flags & flags) > 0
