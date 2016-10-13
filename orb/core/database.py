import blinker
import demandimport
import logging

from collections import defaultdict

with demandimport.enabled():
    import orb

log = logging.getLogger(__name__)


class Database(object):
    """
    Defines a description class for a backend connection.  The `Database`
    class will maintain the connection requirement information that a backend
    connection will use to communicate with the raw server.
    """

    # define database signals
    about_to_connect = blinker.Signal()
    connected = blinker.Signal()
    disconnected = blinker.Signal()

    def __init__(self,
                 connection=None,
                 code='',
                 username='',
                 password='',
                 host=None,
                 port=None,
                 name=None,
                 write_host=None,
                 timeout=20000,
                 credentials=None,
                 system=None):

        # define custom properties
        self.__connection = None
        self.__code = code
        self.__name = name
        self.__host = host
        self.__write_host = write_host
        self.__port = port
        self.__username = username
        self.__password = password
        self.__credentials = credentials
        self.__timeout = timeout  # ms
        self.__system = None

        # setup the database connection
        self.set_connection(connection)

        # auto-register if the system was provided
        if system:
            self.set_system(system)

    def __del__(self):
        self.disconnect()

    def _create_tables(self, connection, tables, context):
        """
        Creates new tables on the backend connection.

        :param connection: <orb.Connection>
        :param tables: [<orb.Table>, ..]
        :param context: <orb.Context>
        """
        namespaces = set()
        info = connection.schema_info(context)

        # create new models
        for table in tables:
            schema = table.schema()
            if schema.dbname() in info:
                continue
            else:
                # ensure the namespace exists
                namespace = schema.namespace(context=context)
                if namespace and namespace not in namespaces:
                    connection.create_namespace(namespace, context)
                    namespaces.add(namespace)

                # create the new table
                connection.create_model(table,
                                        context,
                                        include_references=False,
                                        owner=self.username())

    def _create_views(self, connection, views, context):
        """
        Creates new views on the backend connection.

        :param connection: <orb.Connection>
        :param views: [<orb.View>, ..]
        :param context: <orb.Context>
        """
        namespaces = set()
        for view in views:
            namespace = view.schema().namespace(context=context)
            if namespace and namespace not in namespaces:
                connection.create_namespace(namespace, context)
                namespaces.add(namespace)

            # create the new view
            connection.create_model(view, context)

    def _update_tables(self, connection, tables, context):
        """
        Updates existing tables based on the backend connection.

        :param connection: <orb.Connection>
        :param tables: [<orb.Table>, ..]
        :param context: <orb.Context>
        """
        # update after any newly created tables get generated
        info = connection.schema_info(context)

        virtual_flag = orb.Column.Flags.Virtual

        for table in tables:
            schema = table.schema()
            if not schema.dbname() in info:
                continue
            else:
                model_info = info[schema.dbname()]

                db_fields = model_info['fields'] or []
                db_indexes = model_info['indexes'] or []

                columns = schema.columns(recurse=False, flags=~virtual_flag).values()
                indexes = schema.indexes(recurse=False, flags=~virtual_flag).values()

                # collect the missing columns and indexes
                add = {
                    'fields': [col for col in columns if col.field() not in db_fields],
                    'indexes': [index for index in indexes if index.dbname() not in db_indexes]
                }

                # alter the model with the new indexes
                if add['fields'] or add['indexes']:
                    connection.alter_model(table, context, add=add, owner=self.username())

    def _sync_models(self, connection, models, context):
        """
        Runs the background logic for syncing the database models from the
        system to the connection.

        :param connection: <orb.Connection>
        :param models: [subclass of <orb.Model>, ..]
        :param context: <orb.Context>
        """
        abstract_flag = orb.Schema.Flags.Abstract

        # break models into tables and views (tables get created first)
        tables = []
        views = []
        for model in models:
            if model.schema().test_flag(abstract_flag):
                continue
            elif issubclass(model, orb.View):
                views.append(model)
            elif issubclass(model, orb.Table):
                tables.append(model)
            else:  # pragma: no cover
                raise RuntimeError('Invalid model type found')

        # sync the different processes
        self._create_tables(connection, tables, context)
        self._update_tables(connection, tables, context)
        self._create_views(connection, views, context)

    def activate(self):
        """
        Activates this database by registering it to the system that
        the database is associated with as the currenct connection.

        :return: <bool>
        """
        # check if we already have a system, and if not, register to
        # the global system instance
        if self.__system is None:
            self.set_system(orb.system)

        return self.__system.activate(self)

    def create_namespace(self, namespace, **context):
        """
        Proxy method for the database connection's `create_namespace`
        method.

        :param namespace: <str>

        :return: <bool> modified
        """
        context = orb.Context(**context)
        conn = self.connection()
        return conn.create_namespace(namespace, context)

    def code(self):
        """
        Returns the ID code for this database.  Using codes for different database instances will allow
        you to define multiple schema types for more complex systems.

        :return: <str>
        """
        return self.__code

    def connection(self):
        """
        Returns the backend Connection plugin instance for this database.
        If no connection type has yet been associated with this database, this
        method will raise a `BackendNotFound` error.

        :return: <orb.Connection>
        """
        if self.__connection is None:
            raise orb.errors.BackendNotFound('No backend defined')
        else:
            return self.__connection

    def connection_type(self):
        """
        Returns the plugin type that this connection is.

        :return: <str>
        """
        return self.connection().get_plugin_name()

    def cleanup(self):
        """
        Cleans up the database.  This should be called after large amounts
        of modifications, and is specific to the backend as to how necessary
        it is.
        """
        self.connection().cleanup()

    def credentials(self):
        """
        Returns the credentials for this database.  If not explicitly set,
        this will be a combination of the username, password and application
        token.
        
        :return: (<str> client_secret, <str> client_id)
        """
        return self.__credentials or (self.username(), self.password())

    def disconnect(self):
        """
        Proxy method for this database connection's close method.
                    
        :return: <bool>
        """
        return self.connection().close()

    def host(self):
        """
        Returns the host location assigned to this
        database object.
        
        :return: <str>
        """
        return self.__host

    def interrupt(self, thread_id=None):
        """
        Proxy for this database connection's `interrupt` method.
        
        :param thread_id: <int> or None

        :return: <bool>
        """
        return self.connection().interrupt(thread_id)

    def is_connected(self):
        """
        Proxy for this database connection's `is_connected` method.
        
        :return: <bool>
        """
        return self.connection().is_connected()

    def timeout(self):
        """
        Returns the maximum number of milliseconds to allow a query to occur before timing it out.

        :return: <int> milliseconds
        """
        return self.__timeout

    def name(self):
        """
        Returns the database name for this database instance.  This
        will be what the connection is requried to connect to, and does
        not need to be the identifier (`code`).  If no name is explicitly
        set however, the `code` will be used.
        
        :return: <str>
        """
        return self.__name or self.code()

    def password(self):
        """
        Returns the password used for this database instance.
        
        :return: <str>
        """
        return self.__password

    def port(self):
        """
        Returns the port number to connect to the host on.
        
        :return: <int>
        """
        return self.__port

    def set_connection(self, connection):
        """
        Assigns the backend connection for this database instance.

        :param connection: <str> or <orb.Connection> or None
        """
        # for a None connection, don't bother doing anything special
        if connection is None:
            pass

        # for a non-connection instance, generate the connection
        # based off the plugin
        elif not isinstance(connection, orb.Connection):
            conn = orb.Connection.get_plugin(connection)
            if not conn:
                raise orb.errors.BackendNotFound(connection)
            connection = conn(self)

        # otherwise, associate the database to the connection
        else:
            connection.set_database(self)

        self.__connection = connection

    def set_credentials(self, credentials):
        """
        Sets the credentials for this database to the inputted argument
        list.  This is most often used with the REST based backends.

        :param credentials: (<str> client_id, <str> client_secret) or None
        """
        self.__credentials = credentials

    def set_host(self, host):
        """
        Sets the host path location assigned to this
        database object.  By default, this value will be used
        for both reads and writes.  To set a write specific host,
        use the set_write_host method.  If set to `None`, the
        backend connection will choose an appropriate default (normally
        `localhost`)

        :param host: <str> or None
        """
        self.__host = host

    def set_name(self, name):
        """
        Sets the database name that will be used at the lower level to manage \
        connections to various backends.

        :param name: <str>
        """
        self.__name = name

    def set_password(self, password):
        """
        Sets the password for the connection for this database.

        :param password: <str>
        """
        self.__password = password

    def set_port(self, port):
        """
        Sets the port number to connect to.  If the port is `None`, then
        the backend connection will choose an appropriate default.

        :param port: <int> or None
        """
        self.__port = port

    def set_system(self, system):
        """
        Sets the system that this database is associated with.

        :param system: <orb.System> or None
        """
        # no need for change
        if system is self.__system:
            return

        # need to unregister this database from the original system
        # on a change
        elif self.__system is not None:
            self.__system.unregister(self)

        self.__system = system

        # will register this database to the system if changed
        if system is not None:
            system.register(self)

    def set_timeout(self, timeout):
        """
        Sets the maximum number of milliseconds to allow a query to run on
        the server before canceling it.

        :param timeout: <int> milliseconds
        """
        self.__timeout = timeout

    def set_username(self, username):
        """
        Sets the username used for this database connection.
        
        :param username: <str>
        """
        self.__username = username

    def set_write_host(self, host):
        """
        Sets the host to use for write operations.  This can be used
        if you want to explicitly separate where writes go from reads.

        :param host: <str>
        """
        self.__write_host = host

    def system(self):
        """
        Returns the system this database is associated with.

        :return: <orb.System>
        """
        return self.__system

    def sync(self, models=None, **context):
        """
        Syncs the database by calling its schema sync method.  If
        no specific schema has been set for this database, then
        the global database schema will be used.  If the dryRun
        flag is specified, then all the resulting commands will
        just be logged to the current logger and not actually 
        executed on the database.

        :param models: [subclass of <orb.Model>, ..] or None

        :return: <bool> success
        """
        context = orb.Context(**context)
        conn = self.connection()

        # notify that the connection is about to sync
        event = orb.events.SyncEvent(context=context)
        conn.about_to_sync.send(conn, event=event)
        if event.preventDefault:
            return False

        # if no models were provided, then plan to sync all models
        if models is None:
            models = self.system().models().values()

        # order the models based on their heirarchy
        models.sort(key=lambda x: x.schema())

        # notify the models before the sync
        accepted_models = []
        for model in models:
            event = orb.events.SyncEvent(model=model)
            model.about_to_sync.send(model, event=event)
            if not event.preventDefault:
                accepted_models.append(model)

        # sync the backend models
        self._sync_models(conn, accepted_models, context)

        # notify the models after the sync
        for model in accepted_models:
            event = orb.events.SyncEvent(model=model)
            model.synced.send(model, event=event)

        # notify that the connection has finished syncing
        conn.synced.send(conn, event=event)
        return True

    def username(self):
        """
        Returns the username used for the backend of this
        instance.
        
        :return: <str>
        """
        return self.__username

    def write_host(self):
        """
        Returns the host used for write operations.

        :return: <str>
        """
        return self.__write_host or self.__host
