#!/usr/bin/python

""" Defines the common errors for the database module. """

# define authorship information
__authors__ = ['Eric Hulser']
__author__ = ','.join(__authors__)
__credits__ = []
__copyright__ = 'Copyright (c) 2011, Projex Software'
__license__ = 'LGPL'

# maintanence information
__maintainer__ = 'Projex Software'
__email__ = 'team@projexsoftware.com'

from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr

orb = lazy_import('orb')

# ------------------------------------------------------------------------------

class OrbError(StandardError):
    """ Defines the base error class for the orb package """
    pass

class DatabaseError(OrbError):
    """ Defines the base error class for all database related errors """

    def __init__(self, msg='Unknown database error occurred.'):
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

# B
# ------------------------------------------------------------------------------

class BackendNotFound(OrbError):
    def __init__(self, backend):
        super(BackendNotFound, self).__init__('Could not find %s backend', backend)

# C
# ------------------------------------------------------------------------------

class CannotDelete(OrbError):
    def __init__(self, msg, tablename):
        super(CannotDelete, self).__init__(msg, tablename)

class ColumnNotFound(OrbError):
    def __init__(self, table, column):
        super(ColumnNotFound, self).__init__('Did not find {0} column on {1}.'.format(column, table))

class ColumnReadOnly(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)

        super(ColumnReadOnly, self).__init__('{0} is a read-only column.'.format(text))

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

        super(ColumnRequired, self).__init__('{0} is a required column.'.format(text))

class ConnectionFailed(OrbError):
    def __init__(self, msg, db):

        msgs = [msg]
        msgs.append('')

        pwd = '*' * (len(db.password()) - 4) + db.password()[-4:]

        msgs.append('type: %s' % db.databaseType())
        msgs.append('database: %s' % db.databaseName())
        msgs.append('username: %s' % db.username())
        msgs.append('password: %s' % pwd)
        msgs.append('host: %s' % db.host())
        msgs.append('port: %s' % db.port())

        msgs.append('')

        typs = ','.join(orb.Connection.addons().keys())

        msgs.append('valid types: %s' % typs)

        super(ConnectionFailed, self).__init__('\n'.join(msgs))


class ConnectionLost(OrbError):
    def __init__(self):
        OrbError.__init__(self, 'Connection was lost to the database.  ' \
                                'Please retry again soon.')


# D
# ------------------------------------------------------------------------------

class DatabaseNotFound(OrbError):
    def __init__(self):
        super(DatabaseNotFound, self).__init__('No database was found.')

class DependencyNotFound(OrbError):
    def __init__(self, package):
        msg = 'Required package `{0}` is not installed.'.format(package)
        super(DependencyNotFound, self).__init__(msg)

class DuplicateColumnFound(OrbError):
    """ Thrown when there is a duplicate column found within a single \
        hierarchy of a Table. """

    def __init__(self, schema, column):
        msg = '{0}: {1} is already a column and cannot be duplicated.'
        super(DuplicateColumnFound, self).__init__(msg.format(schema, column))

# I
#------------------------------------------------------------------------------

class Interruption(StandardError):
    def __init__(self):
        StandardError.__init__(self, 'Database operation was interrupted.')

class InvalidResponse(OrbError):
    def __init__(self, method, err):
        msg = 'Invalid response from rest method "{0}": {1}'
        super(InvalidResponse, self).__init__(msg.format(method, err))

class IndexValidationError(ValidationError):
    def __init__(self, index, msg):
        super(IndexValidationError, self).__init__(msg, context=index.schema().name())
        self.index = index

# Q
#------------------------------------------------------------------------------

class QueryFailed(OrbError):
    def __init__(self, sql, options, err):
        msg = 'Query was:\n\n"%s"\n\nArgs: %s\n\nError: %s'
        msg %= (sql, options, err)
        super(QueryFailed, self).__init__(msg)

class QueryInvalid(OrbError):
    def __init__(self, msg):
        super(QueryInvalid, self).__init__(msg)

class QueryIsNull(OrbError):
    def __init__(self):
        super(EmptyQuery, self).__init__('This query will result in no items.')

class QueryTimeout(DatabaseError):
    def __init__(self, query, msecs):
        msg = 'Query took longer than {0} seconds:\n{1}'.format(msecs, query)
        super(QueryTimeout, self).__init__(msecs)

# P
#------------------------------------------------------------------------------

class PrimaryKeyNotDefined(OrbError):
    def __init__(self, record):
        super(OrbError, self).__init__('No primary key defined for {0}.'.format(record))

# R
#------------------------------------------------------------------------------

class RecordNotFound(OrbError):
    def __init__(self, model, pk):
        msg = 'Could not find record {0}({1}).'.format(model.schema().name(), pk)
        super(RecordNotFound, self).__init__(msg)

class ReferenceNotFound(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)

        msg = '{0} is a foreign key with no reference table.'
        super(ReferenceNotFound, self).__init__(msg.format(text))

# T
#------------------------------------------------------------------------------

class TableNotFound(OrbError):
    def __init__(self, table):
        super(TableNotFound, self).__init__('Could not find `{0}` table.'.format(table))

# V
#------------------------------------------------------------------------------

class ValueNotFound(OrbError):
    def __init__(self, record, column):
        super(ValueNotFound, self).__init__('{0} has no value for {1}'.format(record, column))