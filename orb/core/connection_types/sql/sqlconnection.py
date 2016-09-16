"""
Defines the base abstract SQL connection for all SQL based
connection backends. """

import datetime
import contextlib
import logging
import orb
import sys

from abc import abstractmethod
from collections import defaultdict

log = logging.getLogger(__name__)

from .sqlstatement import SQLStatement


# noinspection PyAbstractClass,PyProtectedMember
class SQLConnection(orb.Connection):
    """
    Creates a SQL based backend connection type for handling database
    connections to different SQL based databases.  This class can be subclassed
    to define different SQL connections.f
    """

    def __init__(self, database):
        super(SQLConnection, self).__init__(database)

        # determine the connection pooling type
        if orb.system.settings().worker_class == 'gevent':
            from gevent.queue import Queue
        else:
            from Queue import Queue

        # define custom properties
        self.__batchSize = 500
        self.__maxSize = int(orb.system.settings().max_connections)
        self.__poolSize = defaultdict(lambda: 0)
        self.__pool = defaultdict(Queue)

    # ----------------------------------------------------------------------
    #                       EVENTS
    # ----------------------------------------------------------------------
    def onSync(self, event):
        """
        Initializes the database by defining any additional structures that are required during selection.
        """
        SETUP = self.statement('SETUP')
        if SETUP:
            sql, data = SETUP(self.database())
            if event.context.dryRun:
                print sql % data
            else:
                self.execute(sql, data, writeAccess=True)

    # ----------------------------------------------------------------------
    #                       PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _closed(self, native):
        return native.closed

    @abstractmethod
    def _execute(self,
                 native,
                 command,
                 data=None,
                 returning=True,
                 mapper=dict):
        """
        Executes the inputted command into the current \
        connection cursor.

        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
                    returning  | <bool>
                    mapper     | <variant>
                    retries    | <int>

        :return     [{<str> key: <variant>, ..}, ..], <int> rowcount
        """

    @abstractmethod
    def _open(self, db, writeAccess=False):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQL class.

        :return     <variant> | backend specific database connection
        """

    def _commit(self, native):
        native.commit()

    def _close(self, native):
        native.close()

    @abstractmethod
    def _interrupt(self, threadId, native):
        """
        Interrupts the given backend database connection from a separate thread.

        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """

    def _rollback(self, native):
        try:
            native.rollback()
        except Exception:
            if orb.system.settings().worker_class == 'gevent':
                import gevent
                gevent.get_hub().handle_error(native, *sys.exc_info)
            return
        else:
            return native

    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------

    def addNamespace(self, namespace, context):
        CREATE_NAMESPACE = self.statement('CREATE NAMESPACE')
        if CREATE_NAMESPACE:
            sql, data = CREATE_NAMESPACE(namespace)
            if context.dryRun:
                print sql, data
            else:
                self.execute(sql, data)

    def alterModel(self, model, context, add=None, remove=None, owner=''):
        add = add or {'fields': [], 'indexes': []}
        remove = remove or {'fields': [], 'indexes': []}

        # modify the table with the new fields and indexes
        ALTER = self.statement('ALTER')
        data = {}
        sql = []

        alter_sql, alter_data = ALTER(model, add=add['fields'], remove=remove['fields'], owner=owner)
        if alter_sql:
            sql.append(alter_sql)
            data.update(alter_data)

        # create new indexes
        CREATE_INDEX = self.statement('CREATE INDEX')
        for idx in add['indexes']:
            idx_sql, idx_data = CREATE_INDEX(idx)
            if idx_sql:
                sql.append(idx_sql)
                data.update(idx_data)

        if context.dryRun:
            print sql, data
        else:
            self.execute(u'\n'.join(sql), data, writeAccess=True)

    def close(self):
        """
        Closes the connection to the database for this connection.

        :return     <bool> closed
        """
        for pool in self.__pool.values():
            while not pool.empty():
                conn = pool.get_nowait()
                try:
                    self._close(conn)
                except Exception:
                    pass

        # reset the pool size after closing all connections
        self.__poolSize.clear()

    def count(self, model, context):
        """
        Returns the count of records that will be loaded for the inputted
        information.

        :param      model   | <subclass of orb.Model>
                    context | <orb.Context>

        :return     <int>
        """
        SELECT_COUNT = self.statement('SELECT COUNT')

        try:
            sql, data = SELECT_COUNT(model, context)
        except orb.errors.QueryIsNull:
            return 0
        else:
            if context.dryRun:
                print sql % data
                return 0
            else:
                try:
                    rows, _ = self.execute(sql, data)
                except orb.errors.EmptyCommand:
                    rows = []

                return sum([row['count'] for row in rows])

    def commit(self):
        """
        Commits the changes to the current database connection.

        :return     <bool> success
        """
        with self.native(writeAccess=True) as conn:
            if not self._closed(conn):
                return self._commit(conn)

    def createModel(self, model, context, owner='', includeReferences=True):
        """
        Creates a new table in the database based cff the inputted
        schema information.  If the dryRun flag is specified, then
        the SQLConnection will only be logged to the current logger, and not
        actually executed in the database.

        :param      model    | <orb.Model>
                    context   | <orb.Context>

        :return     <bool> success
        """
        CREATE = self.statement('CREATE')
        sql, data = CREATE(model, includeReferences=includeReferences, owner=owner)
        if not sql:
            log.error('Failed to create {0}'.format(model.schema().dbname()))
            return False
        else:
            if context.dryRun:
                print sql % data
            else:
                self.execute(sql, data, writeAccess=True)

            log.info('Created {0}'.format(model.schema().dbname()))
            return True

    def delete(self, records, context):
        """
        Removes the inputted record from the database.

        :param      records  | <orb.Collection>
                    context  | <orb.Context>

        :return     <int> number of rows removed
        """
        # include various schema records to remove
        DELETE = self.statement('DELETE')
        sql, data = DELETE(records, context)

        if context.dryRun:
            print sql % data
            return 0
        else:
            return self.execute(sql, data, writeAccess=True)

    def execute(self,
                command,
                data=None,
                returning=True,
                mapper=dict,
                writeAccess=False,
                dryRun=False,
                locale=None):
        """
        Executes the inputted command into the current \
        connection cursor.

        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
                    returning  | <bool>
                    mapper     | <variant>
                    retries    | <int>

        :return     [{<str> key: <variant>, ..}, ..], <int> rowcount
        """
        command = command.strip()

        if not command:
            raise orb.errors.EmptyCommand()
        elif dryRun:
            print command % data
            raise orb.errors.DryRun()

        # define properties for execution
        data = data or {}
        command = command.strip()
        data.setdefault('locale', locale or orb.Context().locale)
        start = datetime.datetime.now()

        try:
            with self.native(writeAccess=writeAccess) as conn:
                results, rowcount = self._execute(conn,
                                                  command,
                                                  data,
                                                  returning,
                                                  mapper)

        # always raise interruption errors as these need to be handled
        # from a thread properly
        except orb.errors.Interruption:
            delta = datetime.datetime.now() - start
            log.critical('Query took: %s' % delta)
            raise

        # handle any known a database errors with feedback information
        except orb.errors.DatabaseError as err:
            delta = datetime.datetime.now() - start
            log.error(u'{0}: \n {1}'.format(err, command))
            log.error('Query took: %s' % delta)
            raise

        # always raise any unknown issues for the developer
        except StandardError as err:
            delta = datetime.datetime.now() - start
            log.error(u'{0}: \n {1}'.format(err, command))
            log.error('Query took: %s' % delta)
            raise

        delta = (datetime.datetime.now() - start).total_seconds()
        if delta * 1000 < 3000:
            lvl = logging.DEBUG
        elif delta * 1000 < 6000:
            lvl = logging.WARNING
        else:
            lvl = logging.CRITICAL

        log.log(lvl, 'Query took: %s' % delta)

        return results, rowcount

    def insert(self, records, context):
        """
        Inserts the table instance into the database.  If the
        dryRun flag is specified, then the command will be
        logged but not executed.

        :param      records  | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.Context>

        :return     <dict> changes
        """
        INSERT = self.statement('INSERT')
        sql, data = INSERT(records)
        if context.dryRun:
            print sql, data
            return [], 0
        else:
            return self.execute(sql, data, writeAccess=True)

    def batchSize(self):
        """
        Returns the maximum number of records that can be inserted for a single
        insert statement.

        :return     <int>
        """
        return self.__batchSize

    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.

        :return     <bool> connected
        """
        for pool in self.__pool.values():
            if not pool.empty():
                return True
        return False

    @contextlib.contextmanager
    def native(self, writeAccess=False, isolation_level=None):
        """
        Opens a new database connection to the database defined
        by the inputted database.

        :return     <varaint> native connection
        """
        host = self.database().writeHost() if writeAccess else self.database().host()
        conn = self.open(writeAccess=writeAccess)
        try:
            if isolation_level is not None:
                if conn.isolation_level == isolation_level:
                    isolation_level = None
                else:
                    conn.set_isolation_level(isolation_level)
            yield conn
        except Exception:
            if self._closed(conn):
                conn = None
                self.close()
            else:
                conn = self._rollback(conn)
            raise
        else:
            if not self._closed(conn):
                self._commit(conn)
        finally:
            if conn is not None and not self._closed(conn):
                if isolation_level is not None:
                    conn.set_isolation_level(isolation_level)
                self.__pool[host].put(conn)

    def open(self, writeAccess=False):
        """
        Returns the sqlite database for the current thread.

        :return     <variant> || None
        """
        host = self.database().writeHost() if writeAccess else self.database().host()
        pool = self.__pool[host]

        if self.__poolSize[host] >= self.__maxSize or pool.qsize():
            if pool.qsize() == 0:
                log.warning('Waiting for connection to database!!!')
            return pool.get()
        else:
            db = self.database()

            # process a pre-connect event
            event = orb.events.ConnectionEvent()
            db.onPreConnect(event)

            self.__poolSize[host] += 1
            try:
                conn = self._open(self.database(), writeAccess=writeAccess)
            except Exception:
                self.__poolSize[host] -= 1
                raise
            else:
                event = orb.events.ConnectionEvent(success=conn is not None, native=conn)
                db.onPostConnect(event)
                return conn

    def rollback(self):
        """
        Rolls back changes to this database.
        """
        with self.native(writeAccess=True) as conn:
            return self._rollback(conn)

    def schemaInfo(self, context):
        INFO = self.statement('SCHEMA INFO')
        sql, data = INFO(context)
        info, _ = self.execute(sql, data)
        return {table['name']: table for table in info}

    def select(self, model, context):
        SELECT = self.statement('SELECT')
        sql, data = SELECT(model, context)
        if not sql:
            return []
        elif context.dryRun:
            log.info(sql % data)
            return []
        else:
            try:
                return self.execute(sql, data)[0]
            except orb.errors.EmptyCommand:
                return [], 0

    def setBatchSize(self, size):
        """
        Sets the maximum number of records that can be inserted for a single
        insert statement.

        :param      size | <int>
        """
        self.__batchSize = size

    def update(self, records, context):
        """
        Updates the modified data in the database for the
        inputted record.  If the dryRun flag is specified then
        the command will be logged but not executed.

        :param      record   | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.Context>

        :return     <dict> changes
        """
        UPDATE = self.statement('UPDATE')
        sql, data = UPDATE(records)
        if context.dryRun:
            print sql, data
            return [], 0
        else:
            return self.execute(sql, data, writeAccess=True)

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.

        :return     subclass of <orb.core.backends.sql.SQL>
        """
        if code:
            return SQLStatement.byName(code)
        else:
            return SQLStatement

