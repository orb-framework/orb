""" Defines the common errors for the database module. """

from projex.lazymodule import lazy_import

orb = lazy_import('orb')

# ------------------------------------------------------------------------------


class OrbError(StandardError):
    """ Defines the base error class for the orb package """
    pass


class DatabaseError(OrbError):
    """ Defines the base error class for all database related errors """

    def __init__(self, msg=u'Unknown database error occurred'):
        super(DatabaseError, self).__init__(msg)


class DataStoreError(OrbError):
    """ Raised when storage columns are unable to save or restore data to a backend """
    pass


class SchemaError(OrbError):
    """
    Defines base error class for all schema related errors

    :usage

        raise orb.errors.SchemaError('This is a test')
        raise orb.errors.SchemaError('Could not find {column} on {schema}', schema='User', column='username')
        raise orb.errors.SchemaError('Could not find {column} on {schema}',
                                     schema=User.schema(),
                                     column=User.schema().column('username'))
    """
    DEFAULT_MESSAGE = ''

    def __init__(self, msg='', schema='', column=''):
        msg = msg or self.DEFAULT_MESSAGE

        # support receiving an actual schema object
        if isinstance(schema, orb.Schema):
            schema = schema.name()

        # support receiving an actual column object
        if isinstance(column, orb.Column):
            column = column.name()

        msg = msg.format(schema=schema, column=column)
        super(SchemaError, self).__init__(msg)


class ValidationError(OrbError):
    """
    Raised when a column is being set with a value that does not pass validation.
    """

    def __init__(self, msg, context=''):
        super(ValidationError, self).__init__(msg)
        self.context = context


# B
# ------------------------------------------------------------------------------

class BackendNotFound(OrbError):
    """ Raised when defining a database with a backend connection that does not exist """
    def __init__(self, backend):
        msg = u'Could not find {0} backend'.format(backend)
        super(BackendNotFound, self).__init__(msg)


# C
# ------------------------------------------------------------------------------


class CannotDelete(DatabaseError):
    """ Raised when a backend service fails to delete a record """
    pass


class ColumnNotFound(SchemaError):
    """
    Raised by the model when trying to access a column not defined in the schema

    :usage

        raise orb.errors.ColumnNotFound(schema='User', column='username')

    """
    DEFAULT_MESSAGE = u'Did not find {column} column on {schema}'


class ColumnReadOnly(SchemaError):
    """
    Raised by the model when attempting to modify a column that is read-only

    :usage

        raise orb.errors.ColumnReadOnly(schema='User', column='username')
        raise orb.errors.ColumnReadOnly(schema=schema, column=schema.column('username'))

    """
    DEFAULT_MESSAGE = u'{column} of {schema} is a read-only column'


class ColumnTypeNotFound(SchemaError):
    """ Raised when creating a column based on its registered name """
    def __init__(self, typ):
        msg = u'{0} is not a valid column type'
        super(ColumnTypeNotFound, self).__init__(msg.format(typ))


class ColumnValidationError(ValidationError):
    """ Raised during validation of a record by individual columns within it """
    def __init__(self, column, msg):
        super(ColumnValidationError, self).__init__(msg, context=column.name())

        self.column = column


class ContextError(ValidationError):
    """ Raised when providing invalid context information to the Context object """
    pass


class ConnectionFailed(DatabaseError):
    """ Raised when attempting to connect to a backend """
    def __init__(self, msg=u'Failed to connect to database'):
        super(ConnectionFailed, self).__init__(msg)


class ConnectionLost(DatabaseError):
    """
    Raised when attempting to use an open database connection but it has been
    severed from the client side.
    """
    def __init__(self, msg=u'Connection was lost to the database.  Please retry again soon'):
        OrbError.__init__(self, msg)


# D
# ------------------------------------------------------------------------------

class DatabaseNotFound(DatabaseError):
    """ Raised by a connection when attempting to access a database description """
    def __init__(self):
        super(DatabaseNotFound, self).__init__(u'No database was found')


class DuplicateColumnFound(SchemaError):
    """
    Raised when there is a duplicate column found within a
    single hierarchy of a Model.

    :usage

        raise orb.errors.DuplicateColumnFound('User', 'username')
    """
    DEFAULT_MESSAGE = u'{column} of {schema} is already defined and cannot be duplicated'


class DuplicateEntryFound(DatabaseError):
    """ Raised when the backend finds a duplicate entry in its system """
    pass


class DryRun(OrbError):
    """ Raised by the backend when a query is not fully executed due to a dry run """
    pass


# E
# ------------------------------------------------------------------------------


class EmptyCommand(DatabaseError):
    """ Raised when an empty command is given for execution to the backend """
    pass


class EncryptionDisabled(OrbError):
    pass


# I
# ------------------------------------------------------------------------------


class IdNotFound(SchemaError):
    """ Raised when the model does not have an id column defined """
    DEFAULT_MESSAGE = u'No id column found for {schema} model'


class Interruption(DatabaseError):
    """ Raised when a backend connection or process is remotely terminated """
    def __init__(self):
        msg = u'Database operation was interrupted'
        super(Interruption, self).__init__(msg)


class InvalidReference(ValidationError):
    """
    Raised when validating a reference record being assigned to a column

    :usage

        raise InvalidReference('created_by', expects='User', received='UserRole')

    """
    def __init__(self, column, expects='', received=''):
        msg = u'{0} expects {1} records, not {2}'.format(column, expects, received)
        super(InvalidReference, self).__init__(msg)


class InvalidIndexArguments(ValidationError):
    """ Raised when an index is being called with invalid arguments """
    def __init__(self, index, msg):
        super(InvalidIndexArguments, self).__init__(msg, context=index.schema().name())
        self.index = index


# Q
#------------------------------------------------------------------------------

class QueryFailed(OrbError):
    def __init__(self, sql, options, err):
        msg = u'Query was:\n\n"%s"\n\nArgs: %s\n\nError: %s'
        msg %= (sql, options, err)
        super(QueryFailed, self).__init__(msg)


class QueryInvalid(OrbError):
    def __init__(self, msg):
        super(QueryInvalid, self).__init__(msg)


class QueryIsNull(OrbError):
    def __init__(self):
        super(QueryIsNull, self).__init__(u'This query will result in no items')


class QueryTimeout(DatabaseError):
    def __init__(self, query=None, msecs=None, msg=None):
        msg = msg or u'The server cancelled the query because it was taking too long'

        self.query = query
        self.msecs = msecs

        super(QueryTimeout, self).__init__(msg)


# P
#------------------------------------------------------------------------------

class PrimaryKeyNotDefined(OrbError):
    def __init__(self, record):
        super(OrbError, self).__init__(u'No primary key defined for {0}'.format(record))


# R
#------------------------------------------------------------------------------

class RecordNotFound(OrbError):
    def __init__(self, model, pk):
        msg = u'Could not find record {0}({1})'.format(model.schema().name(), pk)
        super(RecordNotFound, self).__init__(msg)


class ReferenceNotFound(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = column

        msg = u'{0} is a foreign key with no reference table'
        super(ReferenceNotFound, self).__init__(msg.format(text))


# S
# -----------------------------------------------------------------------------

class SearchEngineNotFound(OrbError):
    """ Raised by the model when attempting to search for records """
    def __init__(self, name):
        msg = u'Missing search engine: {0}'.format(name)
        super(SearchEngineNotFound, self).__init__(msg)

# T
#------------------------------------------------------------------------------

class ModelNotFound(OrbError):
    def __init__(self, table):
        super(ModelNotFound, self).__init__(u'Could not find `{0}` table'.format(table))


# V
#------------------------------------------------------------------------------

class ValueNotFound(OrbError):
    def __init__(self, record, column):
        super(ValueNotFound, self).__init__(u'{0} has no value for {1}'.format(record, column))

class ValueOutOfRange(ValidationError):
    def __init__(self, column, value, minimum, maximum):
        msg = u'{0} for {1} is out of range.  Value must be '.format(value, column)

        if minimum is not None and maximum is not None:
            msg += u'between {0} and {1}'.format(minimum, maximum)
        elif minimum is not None:
            msg += u'greater than {0}'
        elif maximum is not None:
            msg += u'less than {0}'.format(maximum)

        super(ValueOutOfRange, self).__init__(msg)

class ViewNotFound(OrbError):
    def __init__(self, table, view):
        super(ViewNotFound, self).__init__(u'{0} has no view {1}'.format(table, view))
