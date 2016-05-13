""" Defines the common errors for the database module. """

from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr

orb = lazy_import('orb')

# ------------------------------------------------------------------------------


class OrbError(StandardError):
    """ Defines the base error class for the orb package """
    pass


class DatabaseError(OrbError):
    """ Defines the base error class for all database related errors """

    def __init__(self, msg=u'Unknown database error occurred.'):
        super(DatabaseError, self).__init__(msg)


class DataStoreError(OrbError):
    pass


class ValidationError(OrbError):
    """
    Raised when a column is being set with a value that does not pass
    validation.
    """

    def __init__(self, msg, context=''):
        super(ValidationError, self).__init__(msg)
        self.context = context


# A
# ------------------------------------------------------------------------------

class ActionNotAllowed(OrbError):
    pass

class ArchiveNotFound(OrbError):
    def __init__(self, table):
        msg = u'Could not find archives for the {0} table.'.format(table)
        super(ArchiveNotFound, self).__init__(msg)

# B
# ------------------------------------------------------------------------------

class BackendNotFound(OrbError):
    def __init__(self, backend):
        super(BackendNotFound, self).__init__(u'Could not find {0} backend'.format(backend))


# C
# ------------------------------------------------------------------------------

class CannotDelete(OrbError):
    pass

class ColumnIsVirtual(OrbError):
    def __init__(self, column):
        super(ColumnIsVirtual, self).__init__(u'Cannot access {0} directly, it is virtual'.format(column))

class ColumnNotFound(OrbError):
    def __init__(self, table, column):
        super(ColumnNotFound, self).__init__(u'Did not find {0} column on {1}.'.format(column, table))


class ColumnReadOnly(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)

        super(ColumnReadOnly, self).__init__(u'{0} is a read-only column.'.format(text))


class ColumnValidationError(ValidationError):
    def __init__(self, column, msg):
        super(ColumnValidationError, self).__init__(msg, context=column.name())

        self.column = column


class ColumnRequired(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)

        super(ColumnRequired, self).__init__(u'{0} is a required column.'.format(text))


class ConnectionFailed(OrbError):
    def __init__(self):
        super(ConnectionFailed, self).__init__(u'Failed to connect to database')


class ConnectionLost(OrbError):
    def __init__(self):
        OrbError.__init__(self, u'Connection was lost to the database.  Please retry again soon.')


# D
# ------------------------------------------------------------------------------

class DatabaseNotFound(OrbError):
    def __init__(self):
        super(DatabaseNotFound, self).__init__(u'No database was found.')


class DependencyNotFound(OrbError):
    def __init__(self, package):
        msg = u'Required package `{0}` is not installed.'.format(package)
        super(DependencyNotFound, self).__init__(msg)


class DuplicateColumnFound(OrbError):
    """ Thrown when there is a duplicate column found within a single \
        hierarchy of a Table. """
    def __init__(self, schema, column):
        msg = u'{0}: {1} is already a column and cannot be duplicated.'
        super(DuplicateColumnFound, self).__init__(msg.format(schema, column))


class DuplicateEntryFound(OrbError):
    pass

class DryRun(OrbError):
    pass

# E
# ------------------------------------------------------------------------------

class EncryptionDisabled(OrbError):
    pass

# I
# ------------------------------------------------------------------------------

class IdNotFound(OrbError):
    def __init__(self, name):
        super(IdNotFound, self).__init__(u'No id column found for {0}'.format(name))

class Interruption(StandardError):
    def __init__(self):
        super(Interruption, self).__init__(u'Database operation was interrupted.')

class InvalidContextOption(ValidationError):
    pass

class InvalidReference(ValidationError):
    def __init__(self, column, value_type, expected_type):
        msg = u'{0} expects {1} records, not {2}'.format(column, expected_type, value_type)
        super(InvalidReference, self).__init__(msg)

class InvalidColumnType(OrbError):
    def __init__(self, typ):
        msg = u'{0} is not a valid column type.'
        super(InvalidColumnType, self).__init__(msg.format(typ))

class EmptyCommand(OrbError):
    pass

class InvalidSearch(OrbError):
    pass

class InvalidResponse(OrbError):
    def __init__(self, method, err):
        msg = u'Invalid response from rest method "{0}": {1}'
        super(InvalidResponse, self).__init__(msg.format(method, err))


class IndexValidationError(ValidationError):
    def __init__(self, index, msg):
        super(IndexValidationError, self).__init__(msg, context=index.schema().name())
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
        super(QueryIsNull, self).__init__(u'This query will result in no items.')


class QueryTimeout(DatabaseError):
    def __init__(self, query=None, msecs=None, msg=None):
        msg = msg or u'The server cancelled the query because it was taking too long.'

        self.query = query
        self.msecs = msecs

        super(QueryTimeout, self).__init__(msg)


# P
#------------------------------------------------------------------------------

class PrimaryKeyNotDefined(OrbError):
    def __init__(self, record):
        super(OrbError, self).__init__(u'No primary key defined for {0}.'.format(record))


# R
#------------------------------------------------------------------------------

class RecordNotFound(OrbError):
    def __init__(self, model, pk):
        msg = u'Could not find record {0}({1}).'.format(model.schema().name(), pk)
        super(RecordNotFound, self).__init__(msg)


class ReferenceNotFound(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)

        msg = u'{0} is a foreign key with no reference table.'
        super(ReferenceNotFound, self).__init__(msg.format(text))


# T
#------------------------------------------------------------------------------

class ModelNotFound(OrbError):
    def __init__(self, table):
        super(ModelNotFound, self).__init__(u'Could not find `{0}` table.'.format(table))


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
        super(ViewNotFound, self).__init__(u'{0} has no view {1}.'.format(table, view))
