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
from orb import errors
from projex.decorators import abstractmethod
from projex.contexts import MultiContext
from projex.locks import ReadWriteLock, ReadLocker, WriteLocker
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)

from .abstractsql import SQL


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
        self.__insertBatchSize = 500
        self.__threads = {}
        self.__concurrencyLocks = defaultdict(ReadWriteLock)

        # set standard properties
        self.setThreadEnabled(True)

    # ----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    @abstractmethod()
    def _execute(self,
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
        return [], -1

    @abstractmethod()
    def _open(self, db):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQL class.

        :param      db | <orb.Database>

        :return     <variant> | backend specific database connection
        """
        return None

    @abstractmethod()
    def _interrupt(self, threadId, connection):
        """
        Interrupts the given backend database connection from a separate thread.

        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        pass

    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------

    def close(self):
        """
        Closes the connection to the database for this connection.

        :return     <bool> closed
        """
        cid = threading.current_thread().ident
        for tid, conn in self.__threads.items():
            if tid == cid:
                conn.close()
            else:
                self._interrupt(tid, conn)

        self.__threads.clear()
        return True

    def count(self, table_or_join, lookup, options):
        """
        Returns the count of records that will be loaded for the inputted
        information.

        :param      table_or_join | <subclass of orb.Table> || None
                    lookup        | <orb.LookupOptions>
                    options       | <orb.ContextOptions>

        :return     <int>
        """
        if orb.Table.typecheck(table_or_join) or orb.View.typecheck(table_or_join):
            SELECT_COUNT = self.sql('SELECT_COUNT')
        else:
            SELECT_COUNT = self.sql('SELECT_COUNT_JOIN')

        data = {}
        try:
            cmd = SELECT_COUNT(table_or_join,
                               lookup=lookup,
                               options=options,
                               IO=data)
        except errors.QueryIsNull:
            return 0

        if options.dryRun:
            print cmd % data
            return 0
        else:
            rows, _ = self.execute(cmd, data)
            return sum([row['count'] for row in rows])

    def commit(self):
        """
        Commits the changes to the current database connection.

        :return     <bool> success
        """
        if not (self.isConnected() and self.commitEnabled()):
            return False

        if orb.Transaction.current():
            orb.Transaction.current().setDirty(self)
        else:
            self.nativeConnection().commit()
        return True

    def createTable(self, schema, options):
        """
        Creates a new table in the database based cff the inputted
        schema information.  If the dryRun flag is specified, then
        the SQLConnection will only be logged to the current logger, and not
        actually executed in the database.

        :param      schema    | <orb.TableSchema>
                    info      | <dict>
                    options   | <orb.ContextOptions>

        :return     <bool> success
        """
        # don't create abstract schemas
        if schema.isAbstract():
            name = schema.name()
            log.debug('{0} is an abstract table, not creating'.format(name))
            return False

        CREATE_TABLE = self.sql('CREATE_TABLE')
        data = {}
        cmd = CREATE_TABLE(schema.model(), options=options, IO=data)

        if not options.dryRun:
            self.execute(cmd, data)
            log.info('Created {0} table.'.format(schema.dbname()))
        else:
            print cmd % data

        return True

    def createView(self, schema, options):
        """
        Creates a new table in the database based cff the inputted
        schema information.  If the dryRun flag is specified, then
        the SQLConnection will only be logged to the current logger, and not
        actually executed in the database.

        :param      schema    | <orb.TableSchema>
                    info      | <dict>
                    options   | <orb.ContextOptions>

        :return     <bool> success
        """
        CREATE_VIEW = self.sql('CREATE_VIEW')
        data = {}
        cmd = CREATE_VIEW(schema.model(), options=options, IO=data)

        if not options.dryRun:
            self.execute(cmd, data)
            log.info('Created {0} table.'.format(schema.dbname()))
        else:
            print cmd % data

        return True

    def disableInternals(self):
        """
        Disables the internal checks and update system.  This method should
        be used at your own risk, as it will ignore errors and internal checks
        like auto-incrementation.  This should be used in conjunction with
        the enableInternals method, usually these are used when doing a
        bulk import of data.

        :sa     enableInternals
        """
        super(SQLConnection, self).disableInternals()

        ENABLE_INTERNALS = self.sql('ENABLE_INTERNALS')
        data = {}
        sql = ENABLE_INTERNALS(False, IO=data)

        self.execute(sql, data)

    def distinct(self, table_or_join, lookup, options):
        """
        Returns a distinct set of results for the given information.

        :param      table_or_join | <subclass of orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.ContextOptions>

        :return     {<str> columnName: <list> value, ..}
        """
        lookup.distinct = True
        records = self.select(table_or_join, lookup, options)

        output = defaultdict(set)
        for record in records:
            for column, value in record.items():
                output[column].add(value)

        return output

    def enableInternals(self):
        """
        Enables the internal checks and update system.  This method should
        be used at your own risk, as it will ignore errors and internal checks
        like auto-incrementation.  This should be used in conjunction with
        the disableInternals method, usually these are used when doing a
        bulk import of data.

        :sa     disableInternals
        """
        ENABLE_INTERNALS = self.sql('ENABLE_INTERNALS')
        data = {}
        sql = ENABLE_INTERNALS(True, IO=data)

        self.execute(sql, data)

        super(SQLConnection, self).enableInternals()

    def existingColumns(self, schema, options):
        """
        Looks up the existing columns from the database based on the
        inputted schema and namespace information.

        :param      schema  | <orb.TableSchema>
                    options | <orb.ContextOptions>

        :return     [<str>, ..]
        """
        TABLE_COLUMNS = self.sql('TABLE_COLUMNS')
        data = {}
        sql = TABLE_COLUMNS(schema, options=options, IO=data)
        result = self.execute(sql, data)[0]
        return [x['column_name'] for x in result]

    def execute(self,
                command,
                data=None,
                autoCommit=True,
                autoClose=True,
                returning=True,
                mapper=dict,
                retries=3):
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
        # make sure we don't have an undefined query
        if data and orb.Query.UNDEFINED in data.values():
            return [], 0

        rowcount = 0
        if data is None:
            data = {}

        command = command.strip()
        if not command:
            return [], 0

        if not self.open():
            raise errors.ConnectionFailed('Failed to open connection.',
                                          self.database())

        # when in debug mode, simply log the command to the logger
        elif self.database().commandsBlocked():
            log.info(command)
            return [], rowcount

        results = []
        start = datetime.datetime.now()
        for i in xrange(retries):
            start = datetime.datetime.now()

            try:
                results, rowcount = self._execute(command,
                                                  data,
                                                  autoCommit,
                                                  autoClose,
                                                  returning,
                                                  mapper)
                break

            # always raise interruption errors as these need to be handled
            # from a thread properly
            except errors.Interruption:
                delta = datetime.datetime.now() - start
                log.critical('Query took: %s' % delta)
                raise

            # attempt to reconnect as long as we have enough retries left
            # otherwise raise the error
            except errors.ConnectionLost:
                delta = datetime.datetime.now() - start
                log.error('Query took: %s' % delta)

                if i != (retries - 1):
                    time.sleep(0.25)
                    self.reconnect()
                else:
                    raise

            # handle any known a database errors with feedback information
            except errors.DatabaseError as err:
                delta = datetime.datetime.now() - start
                log.error('Query took: %s' % delta)
                log.error(u'{0}: \n {1}'.format(err, command))
                raise

            # always raise any unknown issues for the developer
            except StandardError as err:
                delta = datetime.datetime.now() - start
                log.error('Query took: %s' % delta)
                log.error(u'{0}: \n {1}'.format(err, command))
                raise

        delta = (datetime.datetime.now() - start).total_seconds()
        if delta * 1000 < 3000:
            log.debug('Query took: %s' % delta)
            log.debug('{0}\n\ndata:{1}'.format(command, data))
        elif delta * 1000 < 6000:
            log.warning('Query took: %s' % delta)
            log.warning(command)
            log.warning('{0}\n\ndata:{1}'.format(command, data))
        else:
            log.error('Query took: %s' % delta)
            log.error(command)
            log.error('{0}\n\ndata:{1}'.format(command, data))

        return results, rowcount

    def insert(self, records, lookup, options):
        """
        Inserts the table instance into the database.  If the
        dryRun flag is specified, then the command will be
        logged but not executed.

        :param      records  | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.ContextOptions>

        :return     <dict> changes
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)

        # wrap the record in a list
        elif orb.Table.recordcheck(records) or orb.View.recordcheck(records):
            records = [records]

        # determine the proper records for insertion
        inserter = defaultdict(list)
        changes = []
        for record in records:
            # make sure we have some data to insert
            rchanges = record.changeset(columns=lookup.columns)
            changes.append(rchanges)

            # do not insert records that already exist
            if options.force:
                pass
            elif record.isRecord() or not rchanges:
                continue

            inserter[record.schema()].append(record)

        cmds = []
        data = {}

        autoinc = options.autoIncrement
        INSERT = self.sql('INSERT')
        INSERTED_KEYS = self.sql('INSERTED_KEYS')

        locks = []
        for schema, schema_records in inserter.items():
            if not schema_records:
                continue

            colcount = len(schema.columns())
            batchsize = self.insertBatchSize()
            size = batchsize / max(int(round(colcount / 10.0)), 1)

            for batch in projex.iters.batch(schema_records, size):
                batch = list(batch)
                icmd = INSERT(schema,
                              batch,
                              columns=lookup.columns,
                              autoincrement=autoinc,
                              options=options,
                              IO=data)
                if icmd:
                    cmds.append(icmd)

            if cmds:
                locks.append(WriteLocker(self.__concurrencyLocks[schema.name()], delay=0.1))

            # for inherited schemas in non-OO tables, we'll define the
            # primary keys before insertion
            if autoinc and INSERTED_KEYS:
                cmd = INSERTED_KEYS(schema, count=len(schema_records), IO=data)
                cmds.append(cmd)

        if not cmds:
            return {}

        cmd = u'\n'.join(cmds)

        if options.dryRun:
            print cmd % data

            if len(changes) == 1:
                return {}
            else:
                return []
        else:
            with MultiContext(*locks):
                results, _ = self.execute(cmd, data, autoCommit=False)

        if not self.commit():
            if len(changes) == 1:
                return {}
            return []

        # update the values for the database
        for i, record in enumerate(records):
            try:
                record.updateOptions(**options.assigned())
                record._updateFromDatabase(results[i])
            except IndexError:
                pass

            record._markAsLoaded(self.database(), columns=lookup.columns)

        if len(changes) == 1:
            return changes[0]
        return changes

    def insertBatchSize(self):
        """
        Returns the maximum number of records that can be inserted for a single
        insert statement.

        :return     <int>
        """
        return self.__insertBatchSize

    def interrupt(self, threadId=None):
        """
        Interrupts the access to the database for the given thread.

        :param      threadId | <int> || None
        """
        cid = threading.current_thread().ident
        if threadId is None:
            cid = threading.current_thread().ident
            for tid, conn in self.__threads.items():
                if tid != cid:
                    conn.interrupt()
                    self.__threads.pop(tid)
        else:
            conn = self.__threads.get(threadId)
            if not conn:
                return

            if threadId == cid:
                conn.close()
            else:
                self._interrupt(threadId, conn)

            self.__threads.pop(threadId)

    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.

        :return     <bool> connected
        """
        return self.nativeConnection() is not None

    def nativeConnection(self):
        """
        Returns the sqlite database for the current thread.

        :return     <variant> || None
        """
        tid = threading.current_thread().ident
        return self.__threads.get(tid)

    def open(self, force=False):
        """
        Opens a new database connection to the database defined
        by the inputted database.

        :return     <bool> success
        """
        tid = threading.current_thread().ident

        # clear out old ids
        for thread in threading.enumerate():
            if not thread.isAlive():
                self.__threads.pop(thread.ident, None)

        conn = self.__threads.get(tid)

        # check to see if we already have a connection going
        if conn and not force:
            return True

        # make sure we have a database assigned to this backend
        elif not self._database:
            raise errors.DatabaseNotFound()

        # open a new backend connection to the database for this thread
        conn = self._open(self._database)
        if conn:
            self.__threads[tid] = conn
            self._database.callbacks().emit(self._database.Signals.Connected, self)
        else:
            self._database.callbacks().emit(self._database.Signals.Disconnected, self)

        return conn is not None

    def reconnect(self):
        """
        Forces a reconnection to the database.
        """
        tid = threading.current_thread().ident
        db = self.__threads.pop(tid, None)
        if db:
            try:
                db.close()
            except StandardError:
                pass

        return self.open()

    def removeRecords(self, remove, options):
        """
        Removes the inputted record from the database.

        :param      remove  | {<orb.Table>: [<orb.Query>, ..], ..}
                    options | <orb.ContextOptions>

        :return     <int> number of rows removed
        """
        if not remove:
            return 0

        # include various schema records to remove
        count = 0
        DELETE = self.sql('DELETE')
        for table, queries in remove.items():
            with WriteLocker(self.__concurrencyLocks[table.schema().name()], delay=0.1):
                for query in queries:
                    data = {}
                    sql = DELETE(table, query, options=options, IO=data)
                    if options.dryRun:
                        print sql % data
                    else:
                        count += self.execute(sql, data)[1]

        return count

    def rollback(self):
        """
        Rolls back changes to this database.
        """
        db = self.nativeConnection()
        if db:
            db.rollback()
            return True
        return False

    def schemaInfo(self, options):
        SCHEMA_INFO = self.sql('SCHEMA_INFO')
        data = {}
        sql = SCHEMA_INFO(options=options, IO=data)
        info = self.execute(sql, data)[0]
        return {table['name']: table for table in info}

    def select(self, table_or_join, lookup, options):
        if orb.Table.typecheck(table_or_join) or orb.View.typecheck(table_or_join):
            # ensure the primary record information is provided for orb.logger.setLevel(orb.logging.DEBUG)ions
            if lookup.columns and options.inflated:
                lookup.columns += [col.name() for col in
                                   table_or_join.schema().primaryColumns()]

            SELECT = self.sql().byName('SELECT')

            schema = table_or_join.schema()
            data = {}
            sql = SELECT(table_or_join,
                         lookup=lookup,
                         options=options,
                         IO=data)

            # if we don't have any command to run, just return a blank list
            if not sql:
                return []
            elif options.dryRun:
                print sql % data
                return []
            else:
                with ReadLocker(self.__concurrencyLocks[schema.name()]):
                    records = self.execute(sql, data)[0]

                store = self.sql().datastore()
                for record in records:
                    for name, value in record.items():
                        column = schema.column(name)
                        record[name] = store.restore(column, value)

                return records
        else:
            raise orb.errors.DatabaseError('JOIN NOT DEFINED')

    def setupDatabase(self, options):
        """
        Initializes the database by defining any additional structures that are required during selection.
        """
        SETUP_DB = self.sql('SETUP_DB')
        data = {}
        try:
            sql = SETUP_DB(IO=data)
        except StandardError as err:
            log.error(str(err))
        else:
            if options.dryRun:
                print sql % data
            else:
                self.execute(sql, data)

    def setInsertBatchSize(self, size):
        """
        Sets the maximum number of records that can be inserted for a single
        insert statement.

        :param      size | <int>
        """
        self.__insertBatchSize = size

    def setRecords(self, schema, records, **options):
        """
        Restores the data for the inputted schema.

        :param      schema  | <orb.TableSchema>
                    records | [<dict> record, ..]
        """
        if not records:
            return

        engine = self.engine()

        # truncate the table
        cmd, dat = engine.truncateCommand(schema)
        self.execute(cmd, dat, autoCommit=False)

        # disable the tables keys
        cmd, dat = engine.disableInternalsCommand(schema)
        self.execute(cmd, dat, autoCommit=False)

        colcount = len(schema.columns())
        batchsize = self.insertBatchSize()
        size = batchsize / max(int(round(colcount / 10.0)), 1)

        # insert the records
        cmds = []
        dat = {}
        setup = {}
        for batch in projex.iters.batch(records, size):
            batch = list(batch)
            icmd, idata = engine.insertCommand(schema,
                                               batch,
                                               columns=options.get('columns'),
                                               autoincrement=False,
                                               setup=setup)
            cmds.append(icmd)
            dat.update(idata)

        self.execute(u'\n'.join(cmds), dat, autoCommit=False)

        # enable the table keys
        cmd, dat = engine.enableInternalsCommand(schema)
        self.execute(cmd, dat)
        self.commit()

    def tableExists(self, schema, options):
        """
        Checks to see if the inputted table class exists in the
        database or not.

        :param      schema  | <orb.TableSchema>
                    options | <orb.ContextOptions>

        :return     <bool> exists
        """
        TABLE_EXISTS = self.sql('TABLE_EXISTS')
        data = {}
        sql = TABLE_EXISTS(schema, options=options, IO=data)
        return bool(self.execute(sql, data, autoCommit=False)[0])

    def update(self, records, lookup, options):
        """
        Updates the modified data in the database for the
        inputted record.  If the dryRun flag is specified then
        the command will be logged but not executed.

        :param      record   | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.ContextOptions>

        :return     <dict> changes
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)

        # wrap the record in a list
        elif orb.Table.recordcheck(records) or orb.View.recordcheck(records):
            records = [records]

        updater = defaultdict(list)
        changes = []
        for record in records:
            rchanges = record.changeset(columns=lookup.columns)
            changes.append(rchanges)

            if options.force:
                pass

            elif not record.isRecord():
                continue

            elif not rchanges:
                continue

            schemas = [record.schema()]

            for schema in schemas:
                updater[schema].append((record, rchanges))

        if not updater:
            if len(records) > 1:
                return []
            else:
                return {}

        cmds = []
        data = {}
        locks = []

        UPDATE = self.sql('UPDATE')

        for schema, changes in updater.items():
            locks.append(WriteLocker(self.__concurrencyLocks[schema.name()], delay=0.1))
            icmd = UPDATE(schema, changes, options=options, IO=data)
            cmds.append(icmd)

        cmd = u'\n'.join(cmds)

        if options.dryRun:
            print cmd % data
            if len(changes) == 1:
                return {}
            else:
                return []
        else:
            with MultiContext(*locks):
                results, _ = self.execute(cmd, data, autoCommit=False)

        if not self.commit():
            if len(changes) == 1:
                return {}
            return []

        # update the values for the database
        for record in records:
            record._markAsLoaded(self.database(),
                                 columns=lookup.columns)

        if len(changes) == 1:
            return changes[0]
        return changes

    def updateTable(self, schema, info, options):
        """
        Determines the difference between the inputted schema
        and the table in the database, creating new columns
        for the columns that exist in the schema and do not
        exist in the database.  If the dryRun flag is specified,
        then the SQLConnection won't actually be executed, just logged.

        :note       This method will NOT remove any columns, if a column
                    is removed from the schema, it will simply no longer
                    be considered part of the table when working with it.
                    If the column was required by the db, then it will need to
                    be manually removed by a database manager.  We do not
                    wish to allow removing of columns to be a simple API
                    call that can accidentally be run without someone knowing
                    what they are doing and why.

        :param      schema     | <orb.TableSchema>
                    options    | <orb.ContextOptions>

        :return     <bool> success
        """
        # determine the new columns
        existing_columns = info['columns']
        all_columns = schema.fieldNames(recurse=False, kind=orb.Column.Kind.Field)
        missing_columns = set(all_columns).difference(existing_columns)

        # determine new indexes
        table_name = schema.dbname()
        existing_indexes = info['indexes'] or []
        all_indexes = [table_name + '_' + projex.text.underscore(index.name().lstrip('by')) + '_idx'
                       for index in schema.indexes(recurse=False)]
        all_indexes += [table_name + '_' + projex.text.underscore(column.indexName().lstrip('by')) + '_idx'
                        for column in schema.columns(recurse=False, kind=orb.Column.Kind.Field)
                        if column.indexed() and not column.primary()]
        missing_indexes = set(all_indexes).difference(existing_indexes)

        # if no columns are missing, return True to indicate the table is
        # up to date
        if not (missing_columns or missing_indexes):
            return True

        columns = [schema.column(col) for col in missing_columns]
        ALTER = self.sql('ALTER_TABLE')
        data = {}
        sql = ALTER(schema, added=columns, options=options, IO=data)

        if options.dryRun:
            print sql % data
        else:
            self.execute(sql, data)
            opts = (schema.name(), ','.join(missing_columns), ','.join(missing_indexes))
            log.info('Updated {0} table, added {1} columns and {2} indexes.'.format(*opts))

        return True

    @classmethod
    def sql(cls, code=''):
        """
        Returns the statement interface for this connection.

        :return     subclass of <orb.core.backends.sql.SQL>
        """
        if code:
            return SQL.byName(code)
        else:
            return SQL

