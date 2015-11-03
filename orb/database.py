""" Defines the base database class. """

# ------------------------------------------------------------------------------

import logging

from multiprocessing.util import register_after_fork
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Database(object):
    """ Contains all the database connectivity information. """
    def __init__(self, connectionType, code='', username='', password='',
                 host='localhost', port=None, name=None, timeout=20000, credentials=None):

        # define custom properties
        conn = orb.Connection.byName(connectionType)
        if not conn:
            raise orb.errors.BackendNotFound(connectionType)

        # define custom properties
        self.__connection = conn.create(self)
        self.__code = code
        self.__name = name
        self.__host = host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__credentials = credentials
        self.__timeout = timeout  # ms

    def __del__(self):
        self.disconnect()

    # ---------------------------------------------------------------
    #                           EVENT METHODS
    # ---------------------------------------------------------------

    def onPreConnect(self, event):
        pass

    def onPostConnect(self, event):
        pass

    def onDisconnect(self, event):
        pass

    # ---------------------------------------------------------------
    #                           PUBLIC METHODS
    # ---------------------------------------------------------------

    def activate(self, manager=None):
        """
        Activates this database within the given orb instance.  This will
        register the database to orb and then set it as the active one.
        
        :param      manager | <orb.Manager>
        """
        manager = manager or orb.system
        manager.activate(self)
        return True

    def code(self):
        """
        Returns the ID code for this database.  Using codes for different database instances will allow
        you to define multiple schema types for more complex systems.

        :return:    <str>
        """
        return self.__code

    def connection(self):
        """
        Returns the backend Connection plugin instance for this database.

        :return     <orb.Connection> || None
        """
        return self.__connection

    def cleanup(self):
        """
        Cleans up the database.  This should be called after large amounts
        of modifications, and is specific to the backend as to how necessary
        it is.
        """
        self.__connection.cleanup()

    def credentials(self):
        """
        Returns the credentials for this database.  If not explicitly set,
        this will be a combination of the username, password and application
        token.
        
        :return     <tuple>
        """
        return self.__credentials or (self.username(), self.password())

    def connect(self):
        """
        Creates the backend instance for this database and connects it to its
        database server.
        
        :sa         backend
        
        :return     <bool> | success
        """
        event = orb.events.ConnectionEvent()
        self.onPreConnect(event)
        if event.preventDefault:
            return False

        # disconnect after a multiprocess fork or this will error out
        register_after_fork(self, self.disconnect)
        success = self.__connection.open()

        event = orb.events.ConnectionEvent(success=success)
        self.onPostConnect(event)
        return success

    def databaseType(self):
        """
        Returns the database type for this instance.
        
        :return     <str>
        """
        return self._databaseType

    def disconnect(self):
        """
        Disconnects the current database connection from the
        network.
                    
        :return     <bool>
        """
        return self.__connection.close()

    def host(self):
        """
        Returns the host location assigned to this
        database object.
        
        :returns    <str>
        """
        return self.__host or 'localhost'

    def interrupt(self, threadId=None):
        """
        Interrupts the thread at the given id.
        
        :param      threadId | <int> || None
        """
        back = self.backend()
        if back:
            back.interrupt(threadId)

    def isConnected(self):
        """
        Returns whether or not the database is connected to its server.
        
        :return     <bool>
        """
        return self.__connection.isConnected()

    def isThreadEnabled(self):
        """
        Returns whether or not threading is enabled for this database.
        
        :return     <bool>
        """
        return self.__connection.isThreadEnabled()

    def timeout(self):
        """
        Returns the maximum number of milliseconds to allow a query to occur before timing it out.

        :return     <int>
        """
        return self.__timeout

    def name(self):
        """
        Returns the database name for this database instance.
        
        :return     <str>
        """
        return self.__name

    def namespace(self):
        """
        Returns the default namespace for this database.  If no namespace
        is defined, then the global default namespace is returned.
        
        :return     <str>
        """
        if self.__namespace is not None:
            return self.__namespace

        return orb.system.namespace()

    def password(self):
        """
        Returns the password used for this database instance.
        
        :return     <str>
        """
        return self.__password

    def port(self):
        """
        Returns the port number to connect to the host on.
        
        :return     <int>
        """
        return self.__port

    def schemas(self, base=None):
        """
        Looks up all the table schemas in the manager that are mapped to \
        this database.
        
        :return     [<TableSchema>, ..]
        """
        return orb.system.databaseSchemas(self, base)

    def setCurrent(self):
        """
        Makes this database the current default database
        connection for working with models.
        
        :param      database        <Database>
        """
        orb.system.setDatabase(self)

    def setName(self, name):
        """
        Sets the database name that will be used at the lower level to manage \
        connections to various backends.
        
        :param      name | <str>
        """
        self.__name = name

    def setDatabaseType(self, databaseType):
        """
        Sets the database type that will be used for this instance.
        
        :param      databaseType | <str>
        """
        self._databaseType = nstr(databaseType)

    def setCredentials(self, credentials):
        """
        Sets the credentials for this database to the inputted argument
        list.  This is most often used with the REST based backends.
        
        :param      credentials | <tuple> || None
        """
        self.__credentials = credentials

    def setDefault(self, state):
        """
        Sets whether or not this database is the default database.
        
        :param      state | <bool>
        """
        self._default = state

    def setTimeout(self, msecs):
        """
        Sets the maximum number of milliseconds to allow a query to run on
        the server before canceling it.

        :param      msecs | <int>
        """
        self.__timeout = msecs

    def setName(self, name):
        """
        Sets the database name for this instance to the given name.
        
        :param      name   <str>
        """
        self.__name = nstr(name)

    def setNamespace(self, namespace):
        """
        Sets the default namespace for this database to the inputted name.
        
        :param      namespace | <str> || None
        """
        self.__namespace = namespace

    def setHost(self, host):
        """
        Sets the host path location assigned to this
        database object.
        
        :param      host      <str>
        """
        self.__host = nstr(host)

    def setPassword(self, password):
        """ 
        Sets the password for the connection for this database.
        
        :param      password    <str>
        """
        self.__password = nstr(password)

    def setPort(self, port):
        """
        Sets the port number to connect to.  The default value
        will be 5432.
        
        :param      port    <int>
        """
        self.__port = port

    def setUsername(self, username):
        """
        Sets the username used for this database connection.
        
        :param      username        <str>
        """
        self.__username = nstr(username)

    def timezone(self, options=None):
        """
        Returns the timezone associated specifically with this database.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the system in general.
        
        :sa     <orb.Manager>

        :param      options | <orb.ContextOptions>

        :return     <pytz.tzfile> || None
        """
        if self.__timezone is None:
            return orb.system.timezone(options)
        return self.__timezone

    def sync(self, **kwds):
        """
        Syncs the database by calling its schema sync method.  If
        no specific schema has been set for this database, then
        the global database schema will be used.  If the dryRun
        flag is specified, then all the resulting commands will
        just be logged to the current logger and not actually 
        executed on the database.
        
        :note       From version 0.6.0 on, this method now accepts a mutable
                    keyword dictionary of values.  You can supply any member 
                    value for either the <orb.LookupOptions> or
                    <orb.ContextOptions>, 'options' for
                    an instance of the <orb.ContextOptions>
        
        :return     <bool> success
        """
        # collect the information for this database
        con = self.__connection
        schemas = self.schemas(orb.TableSchema)
        schemas.sort()

        options = kwds.get('options', orb.ContextOptions(**kwds))

        # initialize the database
        con.setupDatabase(options)
        info = con.schemaInfo(options)

        # first pass will add columns and default columns, but may miss
        # certain foreign key references since one table may not exist before
        # another yet
        with orb.Transaction():
            for schema in schemas:
                if not schema.dbname() in info:
                    con.createTable(schema, options)

            # update after any newly created tables get generated
            info = con.schemaInfo(options)

            # second pass will ensure that all columns, including foreign keys
            # will be generated
            for schema in schemas:
                if schema.dbname() in info:
                    con.updateTable(schema, info[schema.dbname()], options)

            # third pass will generate all the proper value information
            for schema in schemas:
                model = schema.model()
                model.__syncdatabase__()

            # create the views
            for schema in self.schemas(orb.ViewSchema):
                con.createView(schema, options)

    def username(self):
        """
        Returns the username used for the backend of this
        instance.
        
        :return     <str>
        """
        return self.__username
