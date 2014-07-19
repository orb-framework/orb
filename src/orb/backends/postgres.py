#!/usr/bin/python

""" Defines the backend connection class for Postgres databases. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

# define version information (major,minor,maintanence)
__depends__        = ['pyscopg2']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__

#------------------------------------------------------------------------------

import logging
import time
import traceback

from projex.text import nativestring
import projex.errors

from orb             import errors, Orb
from orb.common      import ColumnType
from orb.connection  import Connection
from orb.transaction import Transaction
from orb.table       import Table
from orb.column      import Column
from orb.join        import Join
from orb.query       import Query as Q
from orb.valuemapper import ValueMapper
from orb.common      import CallbackType

log = logging.getLogger(__name__)

try:
    import psycopg2 as pg
    from psycopg2.extras import DictCursor
    from psycopg2.extensions import QueryCanceledError
    
except ImportError:
    QueryCanceledError = errors.DatabaseError
    log.debug('For Postgres backend, download the psycopg2 module')
    pg = None

#------------------------------------------------------------------------------

from orb.backends.sqlbase import SqlBase,\
                                 DEFAULT_SCHEMA_CMDS as SQL_SCHEMA_CMDS,\
                                 DEFAULT_COLUMN_CMDS as SQL_COLUMN_CMDS,\
                                 DEFAULT_TYPE_MAP as SQL_TYPE_MAP

PG_SCHEMA_CMDS = {
'create':
    """
    CREATE TABLE {table} ({columns}{constraints})
    WITH (OIDS=FALSE);
    ALTER TABLE {table} OWNER TO {username};
    """,
'create_inherited':
    """
    CREATE TABLE {table} ({columns}{constraints})
    INHERITS ({inherits})
    WITH (OIDS=FALSE);
    ALTER TABLE {table} OWNER TO {username};
    """,
'exists':
    """
    SELECT table_name FROM information_schema.tables
    WHERE table_schema='public' AND table_name=%(table)s;
    """,
'existing_columns':
    """
    SELECT column_name FROM information_schema.columns
    WHERE table_schema='public' AND table_name=%(table)s;
    """,
'inserted_keys':
    """
    SELECT {column} FROM {table}
    WHERE {column}>=LASTVAL()-{count}+1
    ORDER BY {column} ASC;
    """
}
DEFAULT_SCHEMA_CMDS = SQL_SCHEMA_CMDS.copy()
DEFAULT_SCHEMA_CMDS.update(PG_SCHEMA_CMDS)

PG_COLUMN_CMDS = {
}
DEFAULT_COLUMN_CMDS = SQL_COLUMN_CMDS.copy()
DEFAULT_COLUMN_CMDS.update(PG_COLUMN_CMDS)

PG_TYPE_MAP = {
    ColumnType.Bool:        'BOOLEAN',
    ColumnType.Decimal:     'DECIMAL',
    ColumnType.Double:      'DOUBLE PRECISION',
    ColumnType.Integer:     'INTEGER',
    ColumnType.BigInt:      'BIGINT',
    ColumnType.Enum:        'INTEGER',
    
    ColumnType.Datetime:                'TIMESTAMP WITHOUT TIME ZONE',
    
    # values will be stored as UTC values and converted to the local timezone
    # when extacted, so do not store them in the database with timezones
    ColumnType.DatetimeWithTimezone:    'TIMESTAMP WITHOUT TIME ZONE',
    
    ColumnType.ForeignKey:  'BIGINT',
    
    ColumnType.Interval:    'TIMEDELTA',
    
    ColumnType.Pickle:      'BYTEA',
    ColumnType.ByteArray:   'BYTEA',
    ColumnType.Pickle:      'BYTEA',
    ColumnType.Dict:        'BYTEA',
    ColumnType.Image:       'BYTEA'
}
DEFAULT_TYPE_MAP = SQL_TYPE_MAP.copy()
DEFAULT_TYPE_MAP.update(PG_TYPE_MAP)
for key, val in DEFAULT_TYPE_MAP.items():
    if val == 'VARCHAR':
        DEFAULT_TYPE_MAP[key] = 'CHARACTER VARYING'

#----------------------------------------------------------------------


class Postgres(SqlBase):
    """ 
    Creates a PostgreSQL backend connection type for handling database
    connections to Postgres databases.
    """
    def __init__(self, database):
        super(Postgres, self).__init__(database,
                                       stringWrapper='"',
                                       schemaCommands=DEFAULT_SCHEMA_CMDS,
                                       columnCommands=DEFAULT_COLUMN_CMDS,
                                       typeMap=DEFAULT_TYPE_MAP)
        
        # set standard properties
        self.setObjectOriented(True)
        
        # update the integer engine with a custom primary command
        int_engine = self.columnEngine(ColumnType.Integer)
        int_engine.setCommand('create_primary', '{column} SERIAL')
        int_engine.setCommand('add_primary', 'ADD COLUMN {column} SERIAL')
    
    #----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    def _execute(self, 
                 command, 
                 data       = None,
                 autoCommit = True,
                 autoClose  = True,
                 returning  = True,
                 mapper     = dict):
        """
        Executes the inputed command into the current \
        connection cursor.
        
        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
        
        :return     [{<str> key: <variant>, ..}, ..]
        """
        rowcount = 0
        if data is None:
            data = {}
        
        # when in debug mode, simply log the command to the log
        elif self.database().commandsBlocked():
            log.info(command)
            return [], rowcount
        
        for key, value in data.items():
            data[key] = ValueMapper.mappedValue(value)
        
        # create a new cursor for this transaction
        db = self.backendDb()
        if db is None:
            raise errors.ConnectionLostError()
        
        cursor = db.cursor(cursor_factory=DictCursor)
        
        log.debug('\n'+command%data+'\n')
        
        try:
            cursor.execute(command, data)
            rowcount = cursor.rowcount
        
        # look for a cancelled query
        except QueryCanceledError:
            raise errors.Interruption()
        
        # look for a disconnection error
        except pg.InterfaceError:
            raise errors.ConnectionLostError()
        
        # look for integrity errors
        except (pg.IntegrityError, pg.OperationalError), err:
            try:
                db.rollback()
            except:
                pass
            
            log.debug(traceback.print_exc())
            raise errors.OrbError(nativestring(err))
        
        # connection has closed underneath the hood
        except pg.Error, err:
            try:
                db.rollback()
            except:
                pass
            
            log.error(traceback.print_exc())
            raise errors.OrbError(nativestring(err))
        
        try:
            results = map(mapper, cursor.fetchall())
        except pg.ProgrammingError:
            results = []
        
        if autoCommit:
            self.commit()
        if autoClose:
            cursor.close()
        
        return results, rowcount
        
    def _open(self, db):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        if not pg:
            raise errors.MissingBackend('psycopg2 is not installed.')
        
        dbname  = db.databaseName()
        user    = db.username()
        pword   = db.password()
        host    = db.host()
        if not host:
            host = 'localhost'
        
        port    = db.port()
        if not port:
            port = 5432
        
        # create the python connection
        try:
            backend_db    = pg.connect(database=dbname, 
                                       user=user, 
                                       password=pword, 
                                       host=host, 
                                       port=port)
            return backend_db
        except pg.OperationalError, err:
            log.error(err)
            raise errors.ConnectionError('Failed to connect to Postgres', db)
    
    def _interrupt(self, threadId, backendDb):
        """
        Interrupts the given backend database connection from a separate thread.
        
        :param      threadId  | <int>
                    backendDb | <variant> | backend specific database.
        """
        try:
            backendDb.cancel()
        except pg.Error:
            pass
    
# register the postgres backend
if pg:
    Connection.register('Postgres', Postgres)