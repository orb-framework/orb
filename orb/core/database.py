""" Defines the base database class. """

# ------------------------------------------------------------------------------

import logging

from collections import defaultdict
from multiprocessing.util import register_after_fork
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Database(object):
    """ Contains all the database connectivity information. """
    def __init__(self,
                 connectionType,
                 code='',
                 username='',
                 password='',
                 host=None,
                 port=None,
                 name=None,
                 writeHost=None,
                 timeout=20000,
                 credentials=None):

        # define custom properties
        self.__connection = None
        self.__code = code
        self.__name = name
        self.__host = host
        self.__writeHost = writeHost
        self.__port = port
        self.__username = username
        self.__password = password
        self.__credentials = credentials
        self.__timeout = timeout  # ms

        # setup the connection type
        self.setConnection(connectionType)

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

    def addNamespace(self, namespace, **context):
        """
        Creates a new namespace within this database.

        :param namespace: <str>
        """
        self.connection().addNamespace(namespace, orb.Context(**context))

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
        return self.__host

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
        return self.__name or self.code()

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

    def setName(self, name):
        """
        Sets the database name that will be used at the lower level to manage \
        connections to various backends.
        
        :param      name | <str>
        """
        self.__name = name

    def setCredentials(self, credentials):
        """
        Sets the credentials for this database to the inputted argument
        list.  This is most often used with the REST based backends.
        
        :param      credentials | <tuple> || None
        """
        self.__credentials = credentials

    def setConnection(self, connection):
        """
        Assigns the backend connection for this database instance.

        :param connection: <str> || <orb.Connection>
        """
        # define custom properties
        if not isinstance(connection, orb.Connection):
            conn = orb.Connection.byName(connection)
            if not conn:
                raise orb.errors.BackendNotFound(connection)
            connection = conn(self)
        else:
            connection.setDatabase(self)

        self.__connection = connection

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

    def setHost(self, host):
        """
        Sets the host path location assigned to this
        database object.  By default, this value will be used
        for both reads and writes.  To set a write specific host,
        use the setWriteHost method.
        
        :param      host      <str>
        """
        self.__host = host

    def setName(self, name):
        """
        Sets the database name for this instance to the given name.

        :param      name   <str>
        """
        self.__name = name

    def setPassword(self, password):
        """ 
        Sets the password for the connection for this database.
        
        :param      password    <str>
        """
        self.__password = password

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

    def setWriteHost(self, host):
        """
        Sets the host to use for write operations.

        :param host: <str>
        """
        self.__writeHost = host

    def sync(self, models=None, **context):
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
                    <orb.Context>, 'options' for
                    an instance of the <orb.Context>
        
        :return     <bool> success
        """
        context = orb.Context(**context)

        # collect the information for this database
        conn = self.__connection
        all_models = orb.system.models(orb.Model).values()
        all_models.sort(cmp=lambda x,y: cmp(x.schema(), y.schema()))

        tables = [model for model in all_models if issubclass(model, orb.Table) and
                  not model.schema().testFlags(orb.Schema.Flags.Abstract) and
                  (not models or model.schema().name() in models)]
        views = [model for model in all_models if issubclass(model, orb.View) and
                  not model.schema().testFlags(orb.Schema.Flags.Abstract) and
                  (not models or model.schema().name() in models)]

        # initialize the database
        event = orb.events.SyncEvent(context=context)
        self.__connection.onSync(event)

        namespaces = set()
        info = conn.schemaInfo(context)

        # create new models
        for model in tables:
            if model.schema().dbname() not in info:
                namespace = model.schema().namespace()
                if namespace and namespace not in namespaces:
                    conn.addNamespace(namespace, context)
                    namespaces.add(namespace)

                conn.createModel(model, context, includeReferences=False, owner=self.username())

        # update after any newly created tables get generated
        info = conn.schemaInfo(context)

        for model in tables:
            try:
                model_info = info[model.schema().dbname()]
            except KeyError:
                continue
            else:
                # collect the missing columns and indexes
                add = defaultdict(list)
                for col in model.schema().columns(recurse=False).values():
                    if col.field() not in (model_info['fields'] or []) and not col.testFlag(col.Flags.Virtual):
                        add['fields'].append(col)

                for index in model.schema().indexes(recurse=False).values():
                    if index.dbname() not in (model_info['indexes'] or []):
                        add['indexes'].append(index)

                # alter the model with the new indexes
                if add['fields'] or add['indexes']:
                    conn.alterModel(model, context, add=add, owner=self.username())

        for model in tables:
            # call the sync event
            event = orb.events.SyncEvent(model=model)
            if model.processEvent(event):
                model.onSync(event)

        # sync views last
        for view in views:
            conn.createModel(view, context)
            event = orb.events.SyncEvent(model=view)
            if view.processEvent(event):
                view.onSync(event)

    def username(self):
        """
        Returns the username used for the backend of this
        instance.
        
        :return     <str>
        """
        return self.__username

    def writeHost(self):
        """
        Returns the host used for write operations.

        :return: <str>
        """
        return self.__writeHost or self.__host
