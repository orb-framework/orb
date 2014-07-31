#!/usr/bin/python

""" Defines the common errors for the database module. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

from projex.text import nativestring as nstr

#------------------------------------------------------------------------------

class OrbError(StandardError):
    """ Defines the base error class for the orb package """
    pass

# B
#------------------------------------------------------------------------------

class BackendNotFoundError(OrbError):
    def __init__(self, backend):
        OrbError.__init__(self, 'Could not find %s backend' % backend)

# C
#------------------------------------------------------------------------------

class CannotRemoveError(OrbError):
    def __init__( self, msg ):
        OrbError.__init__( self, msg )

class ColumnNotFoundError(OrbError):
    def __init__(self, column, table=''):
        try:
            table = column.schema().name()
            col = column.name()
        except AttributeError:
            col = nstr(column)
        
        opts = (table, col)
        OrbError.__init__(self, '%s is a missing column from %s.' % opts)

class ColumnReadOnlyError(OrbError):
    def __init__(self, column):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)
        
        OrbError.__init__(self, '%s is a read-only column.' % text)

class ColumnRequiredError(OrbError):
    def __init__( self, column ):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)
        
        OrbError.__init__(self, '%s is a required column.' % text)

class ConnectionError(OrbError):
    def __init__(self, msg, db):
        from orb import Connection
        
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
        
        Connection.init()
        typs = ','.join(Connection.backends.keys())
        
        msgs.append('valid types: %s' % typs)
        
        OrbError.__init__(self, '\n'.join(msgs))

class ConnectionLostError(OrbError):
    def __init__(self):
        OrbError.__init__(self, 'Connection was lost to the database.  '\
                                'Please retry again soon.')

# D
#------------------------------------------------------------------------------

class DatabaseError(OrbError):
    def __init__(self, err):
        text = '%s\n\nUnknown database error occurred.' % err
        OrbError.__init__( self, text )

class DatabaseQueryError(OrbError):
    def __init__(self, sql, options, err):
        msg = 'Query was:\n\n"%s"\n\nArgs: %s\n\nError: %s'
        msg %= (sql, options, err)
        OrbError.__init__(self, msg)

class DatabaseNotFoundError(OrbError):
    def __init__(self):
        OrbError.__init__(self, 'No database was found.')

class DependencyNotFoundError(OrbError):
    pass

class DuplicateColumnWarning(OrbError):
    """ Thrown when there is a duplicate column found within a single \
        hierarchy of a Table. """
    
    def __init__(self, schema, column):
        opts = (schema, column)
        err = '%s: %s is already a column and cannot be duplicated.' % opts
        OrbError.__init__(self, err)

# F
#------------------------------------------------------------------------------

class ForeignKeyMissingReferenceError(OrbError):
    def __init__( self, column ):
        try:
            text = column.name()
        except AttributeError:
            text = nstr(column)
        
        text = '%s is a foreign key with no reference table.' % column
        OrbError.__init__( self, text )

# I
#------------------------------------------------------------------------------

class Interruption(StandardError):
    def __init__(self):
        StandardError.__init__(self, 'interrupted')

class InvalidDatabaseXmlError(OrbError):
    def __init__( self, xml ):
        OrbError.__init__( self, '%s is an invalid XML data set.' % xml )

class InvalidColumnTypeError(OrbError):
    def __init__( self, columnType ):
        text = '%s is an invalid Column type.' % columnType
        OrbError.__init__( self, text )

class InvalidQueryError(OrbError):
    def __init__( self, query ):
        text = 'Invalid lookup info: %s' % query
        OrbError.__init__( self, text )

class InvalidPrimaryKeyError(OrbError):
    def __init__( self, columns, pkey ):
        colnames = ','.join([col.name() for col in columns])
        text     = 'Invalid key: %s | %s' % (colnames, pkey)
        OrbError.__init__( self, text )

class InvalidResponse(OrbError):
    def __init__(self, method, err):
        msg = 'Invalid response from rest method "%s": %s' % (method, err)
        OrbError.__init__(self, msg)

class InvalidSchemaDefinitionError(OrbError):
    def __init__( self, definition ):
        err = '%s is not a valid schema definition type.' % type(definition)
        err += ' A schema must be either a dictionary or a TableSchema.'
        OrbError.__init__( self, err )

# M
#------------------------------------------------------------------------------

class MissingBackend(OrbError):
    pass

class MissingTableShortcut(OrbError):
    def __init__(self, query):
        err = '%s has no table reference for its shortcuts' % query
        OrbError.__init__(self, err)

class MissingTableSchemaWarning(OrbError):
    """ Thrown when a call to the tableSchema method of the \
        class cannot find the requested table schema. """
    def __init__( self, tableName ):
        err = '%s is not a valid table schema.' % tableName
        OrbError.__init__( self, err )

# P
#------------------------------------------------------------------------------

class PrimaryKeyNotDefinedError(OrbError):
    def __init__( self, record ):
        OrbError.__init__( self, 'No primary key defined for %s.' % record )

class PrimaryKeyNotFoundError(OrbError):
    def __init__( self, table, pkey ):
        msg = '%s has not primary key: %s.' % (table, pkey)
        OrbError.__init__( self, msg )

# T
#------------------------------------------------------------------------------

class TableNotFoundError(OrbError):
    def __init__( self, table ):
        OrbError.__init__( self, 'Could not find "%s" table.' % table )

# S
#------------------------------------------------------------------------------

class SchemaNotFoundError(OrbError):
    def __init__( self ):
        OrbError.__init__( self, 'No schema was found to sync.' )

# U
#----------------------------------------------------------------------

class UnsupportedWhereAggregate(OrbError):
    def __init__(self, typ):
        OrbError.__init__(self, '%s is not a supported WHERE clause' % typ)

# V
#------------------------------------------------------------------------------

class ValidationError(OrbError):
    """ 
    Raised when a column is being set with a value that does not pass
    validation.
    """
    def __init__(self, column, value):
        OrbError.__init__(self, column.validatorHelp())