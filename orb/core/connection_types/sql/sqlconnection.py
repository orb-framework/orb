"""
Defines the base abstract SQL connection for all SQL based
connection backends. """

import datetime
import logging
import orb
import projex.iters
import projex.text
import threading
import time

from collections import defaultdict
from projex.decorators import abstractmethod
from projex.contexts import MultiContext
from projex.locks import ReadWriteLock, ReadLocker, WriteLocker

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

        # define custom properties
        self.__batchSize = 500
        self.__connections = {}
        self.__connectionLock = ReadWriteLock()
        self.__concurrencyLocks = defaultdict(ReadWriteLock)


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
    @abstractmethod()
    def _execute(self,
                 native,
                 command,
                 data=None,
                 autoCommit=True,
                 autoClose=True,
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

    @abstractmethod()
    def _open(self):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQL class.

        :return     <variant> | backend specific database connection
        """

    def _commit(self, native):
        native.commit()
        return True

    def _close(self, native):
        native.close()

    @abstractmethod()
    def _interrupt(self, threadId, native):
        """
        Interrupts the given backend database connection from a separate thread.

        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """

    def _rollback(self, native):
        native.rollback()

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

    def alterModel(self, model, context, add=None, remove=None, owner='postgres'):
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
        cid = threading.current_thread().ident
        with WriteLocker(self.__connectionLock):
            for tid, native in self.__connections.items():
                # closes out a connection from the main thread
                if tid == cid:
                    self._close(native)
                # otherwise, interruptes a connection from a calling thread
                else:
                    self._interrupt(native)

            self.__connections.clear()
            return True

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
                rows, _ = self.execute(sql, data)
                return sum([row['count'] for row in rows])

    def commit(self):
        """
        Commits the changes to the current database connection.

        :return     <bool> success
        """
        result = False
        conn = self.native()
        if conn is not None:
            result = self._commit(conn)
        return result

    def createModel(self, model, context, owner='postgres', includeReferences=True):
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

    def execute(self, command, data=None, autoCommit=True, autoClose=True, returning=True,
                mapper=dict, writeAccess=False, retries=0, dryRun=False, locale=None):
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
        rowcount = 0
        data = data or {}
        command = command.strip()
        data.setdefault('locale', locale or orb.Context().locale)
        conn = self.open()
        if conn is None:
            raise orb.errors.ConnectionFailed()

        results = []
        start = datetime.datetime.now()
        for i in xrange(1 + retries):
            start = datetime.datetime.now()

            try:
                results, rowcount = self._execute(conn,
                                                  command,
                                                  data,
                                                  autoCommit,
                                                  autoClose,
                                                  returning,
                                                  mapper)
                break

            # always raise interruption errors as these need to be handled
            # from a thread properly
            except orb.errors.Interruption:
                delta = datetime.datetime.now() - start
                log.critical('Query took: %s' % delta)
                raise

            # attempt to reconnect as long as we have enough retries left
            # otherwise raise the error
            except orb.errors.ConnectionLost:
                delta = datetime.datetime.now() - start
                log.error('Query took: %s' % delta)

                if i != (retries - 1):
                    time.sleep(0.25)
                    self.reconnect()
                else:
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

        log.log(lvl, '{0}\n\ndata:{1}'.format(command, delta))
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

    def interrupt(self, threadId=None):
        """
        Interrupts the access to the database for the given thread.

        :param      threadId | <int> || None
        """
        cid = threading.current_thread().ident

        # interrupt all connections not on current thread
        if threadId is None:
            cid = threading.current_thread().ident
            with WriteLocker(self.__connectionLock):
                for tid, conn in self.__connections.items():
                    if tid != cid:
                        self._interrupt(threadId, conn)
                        self.__connections.pop(tid)

        # interrupt just the given thread
        else:
            with WriteLocker(self.__connectionLock):
                conn = self.__connections.pop(threadId, None)

            if not conn is not None:
                if threadId == cid:
                    self._close(conn)
                else:
                    self._interrupt(threadId, conn)

    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.

        :return     <bool> connected
        """
        return self.native() is not None

    def native(self):
        """
        Returns the sqlite database for the current thread.

        :return     <variant> || None
        """
        with ReadLocker(self.__connectionLock):
            tid = threading.current_thread().ident
            return self.__connections.get(tid)

    def open(self):
        """
        Opens a new database connection to the database defined
        by the inputted database.

        :return     <varaint> native connection
        """
        tid = threading.current_thread().ident

        # clear out inactive connections
        with WriteLocker(self.__connectionLock):
            for thread in threading.enumerate():
                if not thread.isAlive():
                    self.__connections.pop(thread.ident, None)

            conn = self.__connections.get(tid)

        if conn is not None:
            return conn
        else:
            db = self.database()

            # process a pre-connect event
            event = orb.events.ConnectionEvent()
            db.onPreConnect(event)
            if event.preventDefault:
                return None

            conn = self._open(db)

            # process a post-connect event
            event = orb.events.ConnectionEvent(success=conn is not None, native=conn)
            db.onPostConnect(event)
            if not event.preventDefault and event.success:
                with WriteLocker(self.__connectionLock):
                    self.__connections[tid] = conn

                return conn
            else:
                return None

    def reconnect(self):
        """
        Forces a reconnection to the database.
        """
        tid = threading.current_thread().ident

        with WriteLocker(self.__connectionLock):
            native = self.__connections.pop(tid, None)
            if native:
                self._close(native)

        return self.open()

    def rollback(self):
        """
        Rolls back changes to this database.
        """
        native = self.native()
        if native is not None:
            self._rollback(native)
            return True
        else:
            return False

    def schemaInfo(self, context):
        INFO = self.statement('SCHEMA INFO')
        sql, data = INFO(context.namespace or 'public')
        info, _ = self.execute(sql, data)
        return {table['name']: table for table in info}

    def select(self, model, context):
        SELECT = self.statement('SELECT')
        sql, data = SELECT(model, context)
        if not sql:
            return []
        elif context.dryRun:
            print sql % data
            return []
        else:
            with ReadLocker(self.__concurrencyLocks[model.schema().name()]):
                return self.execute(sql, data)[0]

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

