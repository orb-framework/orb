"""
Defines the base connection class that will be used for communication
to the backend databases.
"""

import cPickle
import logging
import projex.iters

from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from projex.addon import AddonManager
from projex.lazymodule import lazy_import

orb = lazy_import('orb')
errors = lazy_import('orb.errors')

log = logging.getLogger(__name__)


class Connection(AddonManager):
    __metaclass__ = ABCMeta
    
    """ 
    Defines the base connection class type.  This class is used to handle
    database transactions and queries.  The Connection class needs to be
    subclassed to handle connection to a particular kind of database backend,
    the backends that are included with the orb package can be found in the
    <orb.backends> package.
    """

    def __init__(self, database):
        super(Connection, self).__init__()

        # define custom properties
        self.__database = database

    def __del__(self):
        """
        Closes the connection when the connection instance is deleted.
        """
        self.close()

    def onSync(self, event):
        pass

    @abstractmethod
    def addNamespace(self, namespace, context):
        """
        Creates a new namespace into this connection.

        :param namespace: <str>
        :param context: <orb.Context>
        """

    @abstractmethod
    def alterModel(self, model, context, add=None, remove=None, owner=''):
        """
        Determines the difference between the inputted table
        and the table in the database, creating new columns
        for the columns that exist in the table and do not
        exist in the database.

        :note       This method will NOT remove any columns, if a column
                    is removed from the table, it will simply no longer
                    be considered part of the table when working with it.
                    If the column was required by the db, then it will need
                    to be manually removed by a database manager.  We do not
                    wish to allow removing of columns to be a simple API
                    call that can accidentally be run without someone knowing
                    what they are doing and why.

        :param      table    | <orb.TableSchema>
                    options  | <orb.Context>

        :return     <bool> success
        """

    @abstractmethod
    def close(self):
        """
        Closes the connection to the database for this connection.
        
        :return     <bool> closed
        """

    @abstractmethod
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return     <bool> success
        """

    @abstractmethod
    def count(self, model, context):
        """
        Returns the number of records that exist for this connection for
        a given lookup and options.
        
        :sa         distinct, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.Context>
        
        :return     <int>
        """

    @abstractmethod
    def createModel(self, model, context, owner='', includeReferences=True):
        """
        Creates a new table in the database based cff the inputted
        table information.
        
        :param      schema   | <orb.TableSchema>
                    options  | <orb.Context>
        
        :return     <bool> success
        """

    def database(self):
        """
        Returns the database instance that this connection is
        connected to.
        
        :return     <Database>
        """
        return self.__database

    @abstractmethod
    def delete(self, records, context):
        """
        Removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.

        :param      table     | <orb.Collection>
                    context   | <orb.Context>

        :return     <int> | number of rows removed
        """

    @abstractmethod
    def execute(self, command, data=None, flags=0):
        """
        Executes the inputted command into the current
        connection cursor.
        
        :param      command  | <str>
                    data     | <dict> || None
                    flags    | <orb.DatabaseFlags>
        
        :return     <variant> returns a native set of information
        """

    @abstractmethod
    def insert(self, records, context):
        """
        Inserts the database record into the database with the
        given values.
        
        :param      records     | <orb.Collection>
                    context     | <orb.Context>
        
        :return     <bool>
        """

    def interrupt(self, threadId=None):
        """
        Interrupts/stops the database access through a particular thread.
        
        :param      threadId | <int> || None
        """

    @abstractmethod
    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.
        
        :return     <bool> connected
        """

    @abstractmethod
    def open(self, force=False):
        """
        Opens a new database connection to the database defined
        by the inputted database.  If the force parameter is provided, then
        it will re-open a connection regardless if one is open already

        :param      force | <bool>

        :return     <bool> success
        """

    @abstractmethod
    def rollback(self):
        """
        Rolls back the latest code run on the database.
        """

    @abstractmethod
    def select(self, model, context):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.Context>
        
        :return     [<variant> result, ..]
        """

    def setup(self, context):
        """
        Initializes the database with any additional information that is required.
        """

    def setDatabase(self, db):
        """
        Assigns the database instance that this connection serves.

        :param db: <orb.Database>
        """
        self.__database = db

    @abstractmethod
    def schemaInfo(self, context):
        """
        Returns the schema information from the database.

        :return     <dict>
        """

    @abstractmethod
    def update(self, records, context):
        """
        Updates the database record into the database with the
        given values.
        
        :param      record  | <orb.Table>
                    options | <orb.Context>
        
        :return     <bool>
        """

