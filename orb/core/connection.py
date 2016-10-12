"""
Defines the base connection class that will be used for communication
to the backend databases.
"""

import blinker
import logging

from abc import ABCMeta, abstractmethod


log = logging.getLogger(__name__)


class Connection(object):
    __metaclass__ = ABCMeta
    
    """ 
    Defines the base connection class type.  This class is used to handle
    database transactions and queries.  The Connection class needs to be
    subclassed to handle connection to a particular kind of database backend,
    the backends that are included with the orb package can be found in the
    <orb.backends> package.

    Signals
    ----
    * synced

    Usage
    ----

        class MyConnection(Connection):
            def __init__(self, database):
                super(MyConnection, self).__init__(database)

                # create event connections
                Connection.synced.connect(self.on_sync, sender=self)

            def on_sync(self, sender, event=None):
                print sender, event
    """
    __plugin_name__ = ''

    # define connection signals
    synced = blinker.Signal()

    def __init__(self, database):
        super(Connection, self).__init__()

        # define custom properties
        self.__database = database

    def __del__(self):
        """
        Closes the connection when the connection instance is deleted.
        """
        self.close()

    @abstractmethod
    def alter_model(self, model, context, add=None, remove=None, owner=''):
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

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        :param add: [<orb.Column> or <orb.Index> or <orb.Collector>, ..] or None
        :param remove: [<orb.Column> or <orb.Index> or <orb.Collector>, ..] or None
        :param owner: <str>

        :return: <bool> success
        """
        return False  # pragma: no cover

    @abstractmethod
    def create_namespace(self, namespace, context):
        """
        Creates a new namespace into this connection.

        :param namespace: <str>
        :param context: <orb.Context>

        :return: <bool> modified
        """
        return False  # pragma: no cover

    @abstractmethod
    def close(self):
        """
        Closes the connection to the database for this connection.
        
        :return: <bool> success
        """
        return False  # pragma: no cover

    @abstractmethod
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return: <bool> success
        """
        return False  # pragma: no cover

    @abstractmethod
    def count(self, model, context):
        """
        Returns the number of records that exist for this connection for
        a given lookup and options.

        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        
        :return: <int>
        """
        return 0  # pragma: no cover

    @abstractmethod
    def create_model(self, model, context, owner='', include_references=True):
        """
        Creates a new table in the database based cff the inputted
        table information.
        
        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        :param owner: <str>
        :param include_references: <bool>
        
        :return: <bool> success
        """
        return False  # pragma: no cover

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

        :param records: <orb.Collection>
        :param context: <orb.Context>

        :return: <int> number of rows removed
        """
        return 0  # pragma: no cover

    @abstractmethod
    def execute(self, command, data=None, flags=0):
        """
        Executes the inputted command into the current
        connection cursor.

        :param command: <str>
        :param data: <dict> or None
        :param flags: <orb.Database.Flags>

        :return: <variant>
        """
        return None  # pragma: no cover

    @abstractmethod
    def insert(self, records, context):
        """
        Inserts the database record into the database with the
        given values.
        
        :param records: <orb.Collection> or [<orb.Model>, ..]
        :param context: <orb.Context>
        
        :return: <bool> success
        """
        return False  # pragma: no cover

    def interrupt(self, thread_id=None):
        """
        Interrupts/stops the database access through a particular thread.
        
        :param thread_id: <int> or None

        :return: <bool> success
        """
        return False   # pragma: no cover

    @abstractmethod
    def is_connected(self):
        """
        Returns whether or not this connection is currently
        active.
        
        :return: <bool>
        """
        return False  # pragma: no cover

    @abstractmethod
    def open(self, force=False):
        """
        Opens a new database connection to the database defined
        by the inputted database.  If the force parameter is provided, then
        it will re-open a connection regardless if one is open already

        :param force: <bool>

        :return: <bool>
        """
        return False  # pragma: no cover

    @abstractmethod
    def rollback(self):
        """
        Rolls back the latest code run on the database.

        :return: <bool> success
        """
        return False  # pragma: no cover

    @abstractmethod
    def select(self, model, context):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.
                    
        :param model: subclass of <orb.Model>
        :param context: <orb.Context>
        
        :return: <variant>
        """
        return []  # pragma: no cover

    def setup(self, context):
        """
        Initializes the database with any additional information that is required.

        :param context: <orb.Context>
        """

    def set_database(self, db):
        """
        Assigns the database instance that this connection serves.

        :param db: <orb.Database>
        """
        self.__database = db

    @abstractmethod
    def schema_info(self, context):
        """
        Returns the schema information from the database.

        :return: <dict>
        """
        return {}  # pragma: no cover

    @abstractmethod
    def update(self, records, context):
        """
        Updates the database record into the database with the
        given values.
        
        :param records: <orb.Collection>
        :param context: <orb.Context>
        
        :return: <bool> changed
        """
        return False  # pragma: no cover

    @classmethod
    def get_plugin_name(cls):
        """
        Returns the plugin name for this column.  By default, this will
        be the name of the column class, minus the trailing `Column` text.
        To override a class's plugin name, set the `__plugin_name__` attribute
        on the class.

        :return: <str>
        """
        return cls.__plugin_name__

    @classmethod
    def get_plugin(cls, plugin_name):
        """
        Returns the plugin class defintion that matches the
        given name.

        :param: <str>

        :return: subclass of <orb.Connection> or None
        """
        for subcls in cls.__subclasses__():
            if subcls.get_plugin_name() == plugin_name:
                output = subcls
            else:
                output = subcls.get_plugin(plugin_name)

            if output is not None:
                return output