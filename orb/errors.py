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
        super(InvalidIndexArguments, self).__init__(msg, context=index.name())
        self.index = index


# Q
# -----------------------------------------------------------------------------


class QueryError(DatabaseError):
    """ Base class for all query related errors """
    pass


class QueryFailed(QueryError):
    """ Raised when a query is failing in the backend """
    def __init__(self, query, data, error):
        msg = u'\n\n'.join((
            u'Query was:',
            query,
            u'Data: {0}'.format(data),
            u'Error: {0}'.format(error)
        ))
        super(QueryFailed, self).__init__(msg)


class QueryInvalid(QueryError):
    """ Raised when rendering a query cannot be completed """
    pass


class QueryIsNull(QueryError):
    """ Raised when a query will result in no values to be retrieved from a backend """
    def __init__(self):
        msg = u'This query will result in no items'
        super(QueryIsNull, self).__init__(msg)


class QueryTimeout(QueryError):
    """ Raised when a query is taking too long to complete """
    def __init__(self, query=None, msecs=None, msg=None):
        msg = msg or u'The server cancelled the query because it was taking too long'

        self.query = query
        self.msecs = msecs

        super(QueryTimeout, self).__init__(msg)


# R
# -----------------------------------------------------------------------------


class RecordNotFound(SchemaError):
    """ Raised when a record is loaded from the database by id, but not found """
    DEFAULT_MESSAGE = u'Could not find record {schema}({column})'


# S
# -----------------------------------------------------------------------------


class SearchEngineNotFound(OrbError):
    """ Raised by the model when attempting to search for records """
    def __init__(self, name):
        msg = u'Missing search engine: {0}'.format(name)
        super(SearchEngineNotFound, self).__init__(msg)


# T
# -----------------------------------------------------------------------------


class ModelNotFound(SchemaError):
    """ Raised when looking for a model but none can be found """
    DEFAULT_MESSAGE = u'Could not find {schema} model'


# V
#------------------------------------------------------------------------------


class ValueOutOfRange(ValidationError):
    """ Raised when a value is not within a given minimum / maximum bound """
    def __init__(self, column, value, minimum=None, maximum=None):
        msg = u'{0} for {1} is out of range.'.format(value, column)

        if minimum is not None and maximum is not None:
            msg += u'  Value must be between {0} and {1}'.format(minimum, maximum)
        elif minimum is not None:
            msg += u'  Value must be greater than {0}'.format(minimum)
        elif maximum is not None:
            msg += u'  Value must be less than {0}'.format(maximum)

        super(ValueOutOfRange, self).__init__(msg)

