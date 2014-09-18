#!/usr/bin/python

""" Defines the backend connection class for PostgreSQL databases. """

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
import orb
import traceback

from orb import errors
from projex.text import nativestring as nstr

from ..abstractconnection import SQLConnection
from ..abstractsql import SQL

log = logging.getLogger(__name__)

try:
    import psycopg2 as pg
    from psycopg2.extras import DictCursor, register_hstore
    from psycopg2.extensions import QueryCanceledError
    
except ImportError:
    log.debug('For PostgreSQL backend, download the psycopg2 module')
    
    QueryCanceledError = errors.DatabaseError
    pg = None

#----------------------------------------------------------------------

class PSQLConnection(SQLConnection):
    """ 
    Creates a PostgreSQL backend connection type for handling database
    connections to PostgreSQL databases.
    """

    #----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    def _execute(self, command, data=None, autoCommit=True, autoClose=True,
                       returning=True, mapper=dict):
        """
        Executes the inputed command into the current \
        connection cursor.
        
        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
        
        :return     [{<str> key: <variant>, ..}, ..], <int> count
        """
        rowcount = 0
        if data is None:
            data = {}
        
        # when in debug mode, simply log the command to the log
        elif self.database().commandsBlocked():
            log.info(command)
            return [], rowcount
        
        # create a new cursor for this transaction
        db = self.nativeConnection()
        if db is None:
            raise errors.ConnectionLostError()
        
        cursor = db.cursor(cursor_factory=DictCursor)

        # register the hstore option
        cursor.execute('CREATE EXTENSION IF NOT EXISTS hstore;')
        register_hstore(cursor, unicode=True)
        
        if log.getEffectiveLevel() == logging.DEBUG:
            log.debug('#-------------------------')
            log.debug(command % data)
            log.debug('#-------------------------')
        
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
            raise errors.OrbError(nstr(err))
        
        # connection has closed underneath the hood
        except pg.Error, err:
            try:
                db.rollback()
            except:
                pass
            
            log.error(traceback.print_exc())
            raise errors.OrbError(nstr(err))
        
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
            return pg.connect(database=dbname,
                              user=user, 
                              password=pword, 
                              host=host, 
                              port=port)
        except pg.OperationalError, err:
            log.error(err)
            raise errors.ConnectionError('Failed to connect to Postgres', db)
    
    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.
        
        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.cancel()
        except pg.Error:
            pass

    #----------------------------------------------------------------------

    def select(self, table_or_join, lookup, options):
        if orb.Table.typecheck(table_or_join):
            # ensure the primary record information is provided for inflations
            if lookup.columns and options.inflateRecords:
                lookup.columns += [col.name() for col in \
                                   table_or_join.schema().primaryColumns()]

            SELECT = self.sql().byName('SELECT')
            
            schema = table_or_join.schema()
            sql, data = SELECT(table_or_join,
                               lookup=lookup,
                               options=options)
            
            # if we don't have any command to run, just return a blank list
            if not sql:
                return []
            elif options.dryRun:
                print sql % data
                return []
            else:
                records = self.execute(sql, data, autoCommit=False)[0]
                
                store = self.sql().datastore()
                
                for record in records:
                    for name, value in record.items():
                        column = schema.column(name)
                        record[name] = store.restore(column, value)
                
                return records
        else:
            raise orb.DatabaseError('JOIN NOT DEFINED')

    #----------------------------------------------------------------------

    @classmethod
    def sql(self, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.backends.sql.SQLStatement>
        """
        if code:
            return SQL.byName('Postgres').byName(code)
        else:
            return SQL.byName('Postgres')

# register the postgres backend
if pg:
    orb.Connection.registerAddon('Postgres', PSQLConnection)

