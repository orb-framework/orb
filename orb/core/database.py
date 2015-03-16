""" Defines the base database class. """

# ------------------------------------------------------------------------------

import logging
import tempfile

from multiprocessing.util import register_after_fork
from projex.enum import enum
from projex.lazymodule import lazy_import
from projex.text import nativestring as nstr
from xml.etree import ElementTree
from projex.callbacks import CallbackSet

log = logging.getLogger(__name__)
orb = lazy_import('orb')
errors = lazy_import('orb.errors')


class Database(object):
    """ Contains all the database connectivity information. """

    Signals = enum(
        Connected='connected(Connection)',
        Disconnected='disconnected(Connection)'
    )

    def __init__(self,
                 typ='SQLite',
                 name='',
                 user='',
                 password='',
                 host='',
                 port=None,
                 databaseName=None,
                 applicationToken='',
                 referenced=False,
                 manager=None,
                 maximumTimeout=20000):

        # define custom properties
        self._callbacks = CallbackSet()
        self._databaseType = typ
        self._cache = None
        self._name = name
        self._databaseName = databaseName
        self._host = host
        self._port = port
        self._username = user
        self._password = password
        self._applicationToken = applicationToken
        self._manager = manager
        self._credentials = None
        self._referenced = referenced
        self._default = False
        self._backend = None
        self._commandsBlocked = False
        self._namespace = None
        self._timezone = None
        self._columnEngines = {}
        self._maximumTimeout = maximumTimeout  # ms

    def __del__(self):
        self.disconnect()

    def activate(self, manager=None):
        """
        Activates this database within the given orb instance.  This will
        register the database to orb and then set it as the active one.
        
        :param      manager | <orb.Manager>
        """
        if manager is None:
            manager = orb.system

        if not manager:
            return False

        manager.registerDatabase(self)
        manager.setDatabase(self)
        return True

    def applicationToken(self):
        """
        Returns the application token that is linked to the current API.  This
        value is used for backends that require communication with a remote
        server.
        
        :return     <str>
        """
        return self._applicationToken

    def backend(self, autoConnect=False):
        """
        Returns the backend Connection plugin instance for this database.
        
        :param      autoConnect     <bool>
        
        :return     <orb.Connection> || None
        """
        if not self._backend:
            # create a new backend connection instance based on this database
            # type
            backend = orb.Connection.create(self)
            if not backend:
                raise errors.BackendNotFound(self._databaseType)

            self._backend = backend

        if self._backend and autoConnect:
            self._backend.open()

        return self._backend

    def backup(self, filename, **options):
        """
        Exports this database to the given filename.  The file will be a 
        zip file containing pickled data from a database that can be 
        then translated to another database format.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        backend = self.backend()
        if backend:
            return backend.backup(filename, **options)
        return False

    def blockCommands(self, state):
        """
        Sets whether or not the database should be blocking
        the calls from hitting the database.  When this is
        on, the backends will simply log the command that is 
        created to the current logger vs. actually executing it.
        
        :sa         commandsBlocked
        
        :param      state   <bool>
        """
        self._commandsBlocked = state

    def cache(self):
        """
        Returns the data cache for this database.

        :return     <orb.caching.DataCache> || None
        """
        return self._cache or orb.system.cache()

    def callbacks(self):
        """
        Returns the callback set for this database instance.

        :return     <projex.callbacks.CallbackSet>
        """
        return self._callbacks

    def cleanup(self):
        """
        Cleans up the database.  This should be called after large amounts
        of modifications, and is specific to the backend as to how necessary
        it is.
        """
        try:
            self.backend().cleanup()
        except AttributeError:
            pass

    def credentials(self):
        """
        Returns the credentials for this database.  If not explicitly set,
        this will be a combination of the username, password and application
        token.
        
        :return     <tuple>
        """
        if self._credentials is not None:
            return self._credentials
        elif self._applicationToken:
            return self._applicationToken, ''
        else:
            return self._username, self._password

    def copyTo(self, other, **options):
        """
        Copies the contents of this database to the inputted other database.
        This method provides a way to map data from one database type to
        another seamlessly.  It will create a backup of the current
        database to a temp file location, and then perform a restore
        operation on the inputted database.
        
        :warning    This will wipe the contents of the inputted database
                    entirely!
        
        :param      other | <orb.Database>
        """
        temp = tempfile.mktemp()
        self.backup(temp, **options)
        other.restore(temp, **options)

    def commandsBlocked(self):
        """
        Returns whether or not the commands are being blocked
        from hitting the database.  When this is on, the backends
        will simply log the command that is created to the
        current logger vs. actually executing it.
        
        :sa         commandsBlocked
        
        :return     <bool> success
        """
        return self._commandsBlocked

    def connect(self):
        """
        Creates the backend instance for this database and connects it to its
        database server.
        
        :sa         backend
        
        :return     <bool> | success
        """
        backend = self.backend()
        if backend:
            # disconnect after a multiprocess fork or this will error out
            register_after_fork(self, self.disconnect)

            return backend.open()
        return False

    def databaseName(self):
        """
        Returns the database name that will be used at the lower level for \
        this database.  If not explicitly set, then the name will be used.
        
        :return     <str>
        """
        if not self._databaseName:
            return self.name()

        return self._databaseName

    def databaseType(self):
        """
        Returns the database type for this instance.
        
        :return     <str>
        """
        return self._databaseType

    def columnEngine(self, column_or_type):
        """
        Returns the column engine associated with this database for the given
        column or column type.  Engines can be linked to individual columns
        or to a column type overall.  If no data engine is linked directly
        to this database, then it will lookup the generic engine for the
        column type from the backend plugin associated with this database.
        
        :param      column_or_type | <orb.Column> || <orb.ColumnType>
        
        :return     <orb.ColumnEngine> || None
        """
        # lookup by the direct column/type
        try:
            return self._columnEngines[column_or_type]
        except KeyError:
            pass

        # lookup by the column type linked for this database
        if type(column_or_type) == orb.Column:
            column_or_type = column_or_type.columnType()
            try:
                return self._columnEngines[column_or_type]
            except KeyError:
                pass

        # lookup the column type from the backend
        # (at this point it will be just a ColumnType as desired
        backend = self.backend()
        if backend:
            return backend.columnEngine(column_or_type)

        return None

    def disconnect(self):
        """
        Disconnects the current database connection from the
        network.
                    
        :return     <bool>
        """
        if not self._backend:
            return False

        self._backend.close()
        return True

    def duplicate(self):
        """
        Creates a new database instance based on this instance.
        
        :return     <orb.Database>
        """
        inst = self.__class__()
        inst._databaseType = self._databaseType
        inst._name = self._name
        inst._databaseName = self._databaseName
        inst._host = self._host
        inst._port = self._port
        inst._username = self._username
        inst._password = self._password
        inst._applicationToken = self._applicationToken
        inst._commandsBlocked = self._commandsBlocked
        inst._namespace = self._namespace
        inst._timezone = self._timezone
        inst._columnEngines = self._columnEngines.copy()
        inst._maximumTimeout = self._maximumTimeout
        return inst

    def host(self):
        """
        Returns the host location assigned to this
        database object.
        
        :returns    <str>
        """
        if not self._host:
            return 'localhost'
        return self._host

    def interrupt(self, threadId=None):
        """
        Interrupts the thread at the given id.
        
        :param      threadId | <int> || None
        """
        back = self.backend()
        if back:
            back.interrupt(threadId)

    def isDefault(self):
        """
        Returns if this is the default database when loading the system.
        
        :return     <bool>
        """
        return self._default

    def isConnected(self):
        """
        Returns whether or not the database is connected to its server.
        
        :return     <bool>
        """
        if self._backend:
            return self._backend.isConnected()
        return False

    def isReferenced(self):
        """
        Returns whether or not this database was loaded from a referenced file.
        
        :return     <bool>
        """
        return self._referenced

    def isThreadEnabled(self):
        """
        Returns whether or not threading is enabled for this database.
        
        :return     <bool>
        """
        con = self.backend()
        if con:
            return con.isThreadEnabled()
        return False

    def maximumTimeout(self):
        """
        Returns the maximum number of milliseconds to allow a query to occur before timing it out.

        :return     <int>
        """
        return self._maximumTimeout

    def name(self):
        """
        Returns the database name for this database instance.
        
        :return     <str>
        """
        return self._name

    def namespace(self):
        """
        Returns the default namespace for this database.  If no namespace
        is defined, then the global default namespace is returned.
        
        :return     <str>
        """
        if self._namespace is not None:
            return self._namespace

        return orb.system.namespace()

    def password(self):
        """
        Returns the password used for this database instance.
        
        :return     <str>
        """
        return self._password

    def port(self):
        """
        Returns the port number to connect to the host on.
        
        :return     <int>
        """
        return self._port

    def restore(self, filename, **options):
        """
        Imports the data from the given filename.  The file will be a 
        zip file containing pickled data from a database that can be 
        then translated to another database format.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        backend = self.backend()
        if backend:
            self.sync()
            return backend.restore(filename, **options)
        return False

    def schemas(self, base=None):
        """
        Looks up all the table schemas in the manager that are mapped to \
        this database.
        
        :return     [<TableSchema>, ..]
        """
        return orb.system.databaseSchemas(self, base)

    def setApplicationToken(self, token):
        """
        Sets the application token for this database to the inputted token.
        
        :param      token | <str>
        """
        self._applicationToken = token

    def setCache(self, cache):
        """
        Sets the data cache for this database.

        :param      cache | <orb.caching.DataCache> || None
        """
        self._cache = cache

    def setCurrent(self):
        """
        Makes this database the current default database
        connection for working with models.
        
        :param      database        <Database>
        """
        orb.system.setDatabase(self)

    def setDatabaseName(self, databaseName):
        """
        Sets the database name that will be used at the lower level to manage \
        connections to various backends.
        
        :param      databaseName | <str>
        """
        self._databaseName = databaseName

    def setDatabaseType(self, databaseType):
        """
        Sets the database type that will be used for this instance.
        
        :param      databaseType | <str>
        """
        self._databaseType = nstr(databaseType)

    def setColumnEngine(self, column_or_type, engine):
        """
        Sets the column engine associated with this database for the given
        column or column type.  Engines can be linked to individual columns
        or to a column type overall.  If no data engine is linked directly
        to this database, then it will lookup the generic engine for the
        column type from the backend plugin associated with this database.
        
        :param      column_or_type | <orb.Column> || <orb.ColumnType>
                    engine         | <orb.ColumnEngine> || None
        """
        self._columnEngines[column_or_type] = engine

    def setCredentials(self, credentials):
        """
        Sets the credentials for this database to the inputted argument
        list.  This is most often used with the REST based backends.
        
        :param      credentials | <tuple> || None
        """
        self._credentials = credentials

    def setDefault(self, state):
        """
        Sets whether or not this database is the default database.
        
        :param      state | <bool>
        """
        self._default = state

    def setMaximumTimeout(self, msecs):
        """
        Sets the maximum number of milliseconds to allow a query to run on
        the server before canceling it.

        :param      msecs | <int>
        """
        self._maximumTimeout = msecs

    def setName(self, name):
        """
        Sets the database name for this instance to the given name.
        
        :param      databaseName   <str>
        """
        self._name = nstr(name)

    def setNamespace(self, namespace):
        """
        Sets the default namespace for this database to the inputted name.
        
        :param      namespace | <str> || None
        """
        self._namespace = namespace

    def setHost(self, host):
        """
        Sets the host path location assigned to this
        database object.
        
        :param      host      <str>
        """
        self._host = nstr(host)

    def setPassword(self, password):
        """ 
        Sets the password for the connection for this database.
        
        :param      password    <str>
        """
        self._password = nstr(password)

    def setPort(self, port):
        """
        Sets the port number to connect to.  The default value
        will be 5432.
        
        :param      port    <int>
        """
        self._port = port

    def setTimezone(self, timezone):
        """
        Sets the timezone associated directly to this database.
        
        :sa     <orb.Manager.setTimezone>
        
        :param     timezone | <pytz.tzfile> || None
        """
        self._timezone = timezone

    def setUsername(self, username):
        """
        Sets the username used for this database connection.
        
        :param      username        <str>
        """
        self._username = nstr(username)

    def timezone(self, options=None):
        """
        Returns the timezone associated specifically with this database.  If
        no timezone is directly associated, then it will return the timezone
        that is associated with the system in general.
        
        :sa     <orb.Manager>

        :param      options | <orb.ContextOptions>

        :return     <pytz.tzfile> || None
        """
        if self._timezone is None:
            return orb.system.timezone(options)
        return self._timezone

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
        con = self.backend()
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

    def toXml(self, xparent):
        """
        Saves this database instance to xml under the inputted parent.
        
        :param      xparent | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xdatabase = ElementTree.SubElement(xparent, 'database')
        xdatabase.set('name', self._name)
        xdatabase.set('default', nstr(self._default))
        xdatabase.set('timeout', str(self._maximumTimeout))

        if self.databaseType():
            xdatabase.set('type', self.databaseType())

        if self._host:
            ElementTree.SubElement(xdatabase, 'host').text = nstr(self._host)
        if self._port:
            ElementTree.SubElement(xdatabase, 'port').text = nstr(self._port)
        if self._username:
            ElementTree.SubElement(xdatabase, 'username').text = self._username
        if self._password:
            ElementTree.SubElement(xdatabase, 'password').text = self._password
        if self._databaseName:
            ElementTree.SubElement(xdatabase,
                                   'dbname').text = self._databaseName
        if self._applicationToken:
            ElementTree.SubElement(xdatabase,
                                   'token').text = self._applicationToken
        return xdatabase

    def username(self):
        """
        Returns the username used for the backend of this
        instance.
        
        :return     <str>
        """
        return self._username

    @staticmethod
    def current(manager=None):
        """
        Returns the current database instance for the given manager.  If
        no manager is provided, then the global system manager will be used.
        
        :param      manager | <orb.Manager> || None
        
        :return     <orb.Database> || None
        """
        if manager is None:
            manager = orb.system
        return manager.database()

    @staticmethod
    def fromXml(xdatabase, referenced=False):
        """
        Returns a new database instance from the inputted xml data.
        
        :param      xdatabase | <xml.etree.Element>
        
        :return     <Database>
        """
        db = Database(referenced=referenced)

        db.setDatabaseType(xdatabase.get('type', 'SQLite'))
        db.setName(xdatabase.get('name', ''))
        db.setDefault(xdatabase.get('default') == 'True')
        db.setMaximumTimeout(int(xdatabase.get('maximumTimeout', 5000)))

        xhost = xdatabase.find('host')
        xport = xdatabase.find('port')
        xuser = xdatabase.find('username')
        xpword = xdatabase.find('password')
        xdbname = xdatabase.find('dbname')
        xtoken = xdatabase.find('token')

        if xhost is not None:
            db.setHost(xhost.text)
        if xport is not None:
            db.setPort(xport.text)
        if xuser is not None:
            db.setUsername(xuser.text)
        if xpword is not None:
            db.setPassword(xpword.text)
        if xdbname is not None:
            db.setDatabaseName(xdbname.text)
        if xtoken is not None:
            db.setApplicationToken(xtoken.text)

        return db

