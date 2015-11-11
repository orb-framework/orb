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
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
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
        pass

    @abstractmethod()
    def _open(self):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQL class.

        :return     <variant> | backend specific database connection
        """
        return None

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
        pass

    def _rollback(self, native):
        native.rollback()

    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------

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
        SQL = self.statement('SELECT COUNT')

        data = {}
        try:
            cmd = SQL(model, context, data)
        except orb.errors.QueryIsNull:
            return 0
        else:
            try:
                rows, _ = self.execute(cmd, data, dryRun=context.dryRun)
                return sum([row['count'] for row in rows])
            except orb.errors.DryRun:
                return 0

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

    def create(self, model, context):
        """
        Creates a new table in the database based cff the inputted
        schema information.  If the dryRun flag is specified, then
        the SQLConnection will only be logged to the current logger, and not
        actually executed in the database.

        :param      model    | <orb.Model>
                    context   | <orb.Context>

        :return     <bool> success
        """
        if model.schema().isAbstract():
            raise orb.errors.CannotCreateModel(model)

        if isinstance(model, orb.Table):
            kind = 'TABLE'
        elif isinstance(model, orb.View):
            kind = 'VIEW'
        else:
            raise orb.errors.CannotCreateModel(model)

        SQL = self.statement('CREATE {0}'.format(kind))
        data = {}
        cmd = SQL(model, context, data)

        try:
            self.execute(cmd, data, dryRun=context.dryRun)
        except orb.errors.DryRun:
            pass

        log.info('Created {0} {1}.'.format(model.schema().dbname(), kind.lower()))
        return True

    def delete(self, records, context):
        """
        Removes the inputted record from the database.

        :param      records  | <orb.Collection>
                    context  | <orb.Context>

        :return     <int> number of rows removed
        """
        # include various schema records to remove
        SQL = self.statement('DELETE')
        data = {}
        cmd = SQL(records, context, data)

        try:
            return self.execute(cmd, data, writeAccess=True, dryRun=context.dryRun)
        except orb.errors.DryRun:
            return 0

    def existingColumns(self, model, context):
        """
        Looks up the existing columns from the database based on the
        inputted schema and namespace information.

        :param      schema  | <orb.TableSchema>
                    options | <orb.Context>

        :return     [<str>, ..]
        """
        TABLE_COLUMNS = self.sql('TABLE_COLUMNS')
        data = {}
        sql = TABLE_COLUMNS(schema, options=options, IO=data)
        result = self.execute(sql, data)[0]
        return [x['column_name'] for x in result]

    def execute(self, command, data=None, autoCommit=True, autoClose=True, returning=True,
                mapper=dict, writeAccess=False, retries=0, dryRun=False):
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

        log.log('{0}\n\ndata:{1}'.format(command, delta))
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
        if isinstance(records, orb.Collection):
            records = list(records)
        elif isinstance(records, orb.Model):
            records = [records]

        # determine the proper records for insertion
        inserter = defaultdict(list)
        changes = []
        for record in records:
            # make sure we have some data to insert
            rchanges = record.changes(columns=context.columns)
            changes.append(rchanges)

            # do not insert records that already exist
            if context.force:
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
            batchsize = self.batchSize()
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
            if not event.preventDefault and conn.success:
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

    def schemaInfo(self, options):
        SCHEMA_INFO = self.sql('SCHEMA_INFO')
        data = {}
        sql = SCHEMA_INFO(options=options, IO=data)
        info = self.execute(sql, data)[0]
        return {table['name']: table for table in info}

    def select(self, model, context):
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
                return records
        else:
            raise orb.errors.DatabaseError('JOIN NOT DEFINED')

    def setup(self, options):
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

    def setBatchSize(self, size):
        """
        Sets the maximum number of records that can be inserted for a single
        insert statement.

        :param      size | <int>
        """
        self.__batchSize = size

    def setRecords(self, model, records, context):
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
        batchsize = self.batchSize()
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

    def tableExists(self, model, context):
        """
        Checks to see if the inputted table class exists in the
        database or not.

        :param      schema  | <orb.TableSchema>
                    options | <orb.Context>

        :return     <bool> exists
        """
        TABLE_EXISTS = self.sql('TABLE_EXISTS')
        data = {}
        sql = TABLE_EXISTS(schema, options=options, IO=data)
        return bool(self.execute(sql, data, autoCommit=False)[0])

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

    def updateTable(self, model, info, context):
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
                    options    | <orb.Context>

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
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.

        :return     subclass of <orb.core.backends.sql.SQL>
        """
        if code:
            return SQLStatement.byName(code)
        else:
            return SQLStatement

