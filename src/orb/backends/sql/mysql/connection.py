#!/usr/bin/python

"""
Defines the backend connection class for MySQL through the
python-mysql backend databases.
"""

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
__depends__        = ['MySQLdb']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__

#------------------------------------------------------------------------------

import logging
import orb

from orb import errors

log = logging.getLogger(__name__)

try:
    import MySQLdb as mysql
    from MySQLdb.cursors import DictCursor
    
except ImportError:
    log.debug('For MySQL backend, ensure python-mysql is installed')
    
    mysql = None
    DictCursor = None

from ..abstractconnection import SQLConnection
from ..abstractsql import SQL


class MySQLConnection(SQLConnection):
    """ 
    Creates a MySQL backend connection type for handling database
    connections to MySQL databases.
    """
    def __init__(self, database):
        super(MySQLConnection, self).__init__(database)
        
        # update the integer engine with a custom primary command
        int_engine = self.columnEngine(orb.ColumnType.Integer)
        int_engine.setCommand('create_primary', '{column} SERIAL')
        int_engine.setCommand('add_primary', 'ADD COLUMN {column} SERIAL')
    
    #----------------------------------------------------------------------
    #                         PROTECTED METHODS
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
        
        def map_value(command, key, value):
            if type(value) in (list, tuple, set):
                data.pop(key, None)
                mapped_items = []
                for i, subvalue in enumerate(value):
                    new_key = '{0}_{1}'.format(key, i)
                    command, new_items = map_value(command, new_key, subvalue)
                    mapped_items += new_items
                
                subkeys = []
                for subkey, subvalue in mapped_items:
                    subkeys.append('%({0})s'.format(subkey))
                    data[subkey] = subvalue
                
                command = command.replace('%({0})s'.format(key),
                                          '('+','.join(subkeys)+')')
                
                return command, mapped_items
            else:
                data[key] = orb.DataConverter.toPython(value)
                return command, [(key, value)]
        
        for key, value in data.items():
            command, items = map_value(command, key, value)
        
        # create a new cursor for this transaction
        db = self.backendDb()
        if not db:
            raise errors.ConnectionLostError()
        
        cursor = db.cursor()
        cursor._defer_warnings = not self.internalsEnabled()
        get_last_row = 'LAST_INSERT_ID()' in command
        command = command.replace('; SELECT LAST_INSERT_ID()', '')
        
        log.debug(command)
        try:
            cursor.execute(command, data)
            rowcount = cursor.rowcount
        
        except mysql.OperationalError, err:
            if err[0] == 1317:
                raise errors.Interruption()
            
            raise
        
        except Exception, err:
            raise
        
        if returning:
            results = map(mapper, cursor.fetchall())
            if not results and get_last_row and cursor.lastrowid:
                results = [{'PRIMARY_KEY': cursor.lastrowid}]
        else:
            results = []
        
        if autoCommit:
            self.commit()
        
        if autoClose:
            cursor.close()
        
        return results, rowcount
    
    def _open(self, db):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :param      db | <orb.Database>
        
        :return     <bool> success
        """
        if not mysql:
            raise errors.MissingBackend('MySQLdb not installed.')
            
        dbname  = db.databaseName()
        user    = db.username()
        pword   = db.password()
        host    = db.host()
        if not host:
            host = 'localhost'
        
        port    = db.port()
        if not port:
            port = 3306
        
        # create the python connection
        try:
            mysqldb = mysql.connect(host=host,
                                    port=port,
                                    user=user,
                                    passwd=pword,
                                    db=dbname)
            mysqldb.cursorclass = DictCursor
            return mysqldb
        
        except mysql.OperationalError, err:
            raise errors.ConnectionError('Could not connect to MySQL', db)
    
    def _interrupt(self, threadId, backendDb):
        """
        Interrupts the given backend database connection from a separate thread.
        
        :param      threadId | <int>
                    backendDb | <variant> | backend specific database.
        """
        backendDb.kill(backendDb.thread_id())
    
    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------
    
    def existingColumns(self, schema, namespace=None, mapper=None):
        """
        Looks up the existing columns from the database based on the
        inputed schema and namespace information.
        
        :param      schema      | <orb.TableSchema>
                    namespace   | <str> || None
        
        :return     [<str>, ..]
        """
        if mapper is None:
            mapper = map(lambda x: x['Field'], results)
        
        return super(MySQLConnection, self).existingColumn(schema, namespace, mapper)

    @classmethod
    def sql(self):
        """
        Returns the statement interface for this connection.
        
        :return     subclass of <orb.backends.sql.SQLStatement>
        """
        return SQL.byName('MySQL')

# register the mysql backend
if mysql:
    orb.Connection.registerAddon('MySQL', MySQLConnection)