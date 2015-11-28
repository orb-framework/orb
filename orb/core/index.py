""" Defines an indexing system to use when looking up records. """

import logging
import projex.text

from xml.etree import ElementTree
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr


log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Index(object):
    """ 
    Defines an indexed way to lookup information from a database.
    Creating an Index generates an object that works like a method, however
    has a preset query built into it, along with caching options.
    """

    def __init__(self, columns=None, name='', dbname='', unique=False, order=None):
        self.__name = self.__name__ = name
        self.__dbname = dbname
        self.__columns = columns or []
        self.__unique = unique
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
                return None if self.__unique else orb.Collection()
            elif not column:
                raise errors.ColumnNotFound(schema.name(), col)

            query &= orb.Query(col) == value

        context['where'] = query & context.get('where')

        records = model.select(**context)
        return records.first() if self.__unique else records

    def columns(self):
        """
        Returns the list of column names that this index will be expecting as \
        inputs when it is called.
        
        :return     [<str>, ..]
        """
        schema = self.schema()
        return [schema.column(col) for col in self.__columns]

    def dbname(self):
        return self.__dbname or orb.system.syntax().indexdb(self.__schema, self.__name)

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

    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name = self.__name__ = name

    def setSchema(self, schema):
        self.__schema = schema

    def setUnique(self, state):
        """
        Sets whether or not this index should find only a unique record.
        
        :param      state | <bool>
        """
        self.__unique = state

    def unique(self):
        """
        Returns whether or not the results that this index expects should be \
        a unique record, or multiple records.
        
        :return     <bool>
        """
        return self.__unique

    def validate(self, record, values):
        """
        Validates whether or not this index's requirements are satisfied by the inputted record and
        values.  If this index fails validation, a ValidationError will be raised.

        :param      record | subclass of <orb.Table>
                    values | {<orb.Column>: <variant>, ..}

        :return     <bool>
        """
        schema = record.schema()
        try:
            column_values = [values[schema.column(name)] for name in self.columns()]
        except StandardError:
            msg = 'Missing some columns ({0}) from {1}.{2}.'.format(', '.join(self.columns()),
                                                                    record.schema().name(),
                                                                    self.name())
            raise errors.IndexValidationError(self, msg=msg)

        # # ensure a unique record is preserved
        # if self.unique():
        #     lookup = getattr(record, self.name())
        #     other = lookup(*column_values)
        #     if other and other != record:
        #         msg = 'A record already exists with the same {0} combination.'.format(', '.join(self.columnNames()))
        #         raise errors.IndexValidationError(self, msg=msg)

        return True

