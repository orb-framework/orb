#!/usr/bin/python

"""
Defines the base abstract SQL connection for all SQL based
connection backends. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

# define version information (major,minor,maintanence)
__depends__        = []
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__


#------------------------------------------------------------------------------

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
from projex.text import nativestring as nstr

log = logging.getLogger(__name__)

from .abstractsql import SQL


class SQLConnection(orb.Connection):
    """ 
    Creates a SQL based backend connection type for handling database
    connections to different SQL based databases.  This class can be subclassed
    to define different SQL connections.
    """
    
    def __init__(self, database):
        super(SQLConnection, self).__init__(database)
        
        # define custom properties
        self.__insertBatchSize = 500
        self.__threads = {}
        
        # set standard properties
        self.setThreadEnabled(True)

    #----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    @abstractmethod()
    def _execute(self, 
                 command, 
                 data       = None,
                 autoCommit = True,
                 autoClose  = True,
                 returning  = True,
                 mapper     = dict):
        """
        Executes the inputed command into the current \
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
        Closes the connection to the datbaase for this connection.
        
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
        Returns the count of records that will be loaded for the inputed 
        information.
        
        :param      table_or_join | <subclass of orb.Table> || None
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     <int>
        """
        if orb.Table.typecheck(table_or_join):
            SELECT_COUNT = self.sql('SELECT_COUNT')
        else:
            SELECT_COUNT = self.sql('SELECT_COUNT_JOIN')
        
        try:
            cmd, data = SELECT_COUNT(table_or_join,
                                     lookup=lookup,
                                     options=options)
        except errors.EmptyQuery:
            return 0
        
        if options.dryRun:
            print cmd % data
            return 0
        else:
            rows, _ = self.execute(cmd, data, autoCommit=False)
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
        Creates a new table in the database based cff the inputed
        schema information.  If the dryRun flag is specified, then
        the SQLConnection will only be logged to the current logger, and not
        actually executed in the database.
        
        :param      schema    | <orb.TableSchema>
                    options   | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        # don't create abstract schemas
        if schema.isAbstract():
            name = schema.name()
            log.debug('{0} is an abstract table, not creating'.format(name))
            return False
        
        CREATE_TABLE = self.sql('CREATE_TABLE')
        cmd, data = CREATE_TABLE(schema, options=options)
        
        if not options.dryRun:
            self.execute(cmd, data)
            log.info('Created {0} table.'.format(schema.tableName()))
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
        sql, data = ENABLE_INTERNALS(False)
        
        self.execute(sql, data, autoCommit=False)
    
    def distinct(self, table_or_join, lookup, options):
        """
        Returns a distinct set of results for the given information.
        
        :param      table_or_join | <subclass of orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
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
        sql, data = ENABLE_INTERNALS(True)
        
        self.execute(sql, data, autoCommit=False)
        
        super(SQLConnection, self).enableInternals()
    
    def existingColumns(self, schema, options):
        """
        Looks up the existing columns from the database based on the
        inputed schema and namespace information.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.DatabaseOptions>
        
        :return     [<str>, ..]
        """
        TABLE_COLUMNS = self.sql('TABLE_COLUMNS')
        sql, data = TABLE_COLUMNS(schema, options=options)
        result = self.execute(sql, data, autoCommit=False)[0]
        return [x['column_name'] for x in result]
    
    def execute(self, 
                command, 
                data       = None,
                autoCommit = True,
                autoClose  = True,
                returning  = True,
                mapper     = dict,
                retries    = 3):
        """
        Executes the inputed command into the current \
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
        rowcount = 0
        if data is None:
            data = {}
        
        command = command.strip()
        if not command:
            return [], 0
        
        if not self.open():
            raise errors.ConnectionError('Failed to open connection.',
                                         self.database())
        
        # when in debug mode, simply log the command to the logger
        elif self.database().commandsBlocked():
            log.info(command)
            return [], rowcount
        
        results = []
        delta = None
        for i in range(retries):
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
                log.debug('Query took: %s' % delta)
                raise
            
            # attempt to reconnect as long as we have enough retries left
            # otherwise raise the error
            except errors.ConnectionLostError, err:
                delta = datetime.datetime.now() - start
                log.debug('Query took: %s' % delta)
                
                if i != (retries - 1):
                    time.sleep(0.25)
                    self.reconnect()
                else:
                    raise
            
            # handle any known a database errors with feedback information
            except errors.DatabaseError, err:
                delta = datetime.datetime.now() - start
                log.debug('Query took: %s' % delta)
                log.error(command)
                
                if self.isConnected():
                    if orb.Transaction.current():
                        orb.Transaction.current().rollback(err)
                    
                    try:
                        self.rollback()
                    except:
                        pass
                    
                    raise
                else:
                    raise
            
            # always raise any unknown issues for the developer
            except StandardError:
                log.error(command)
                delta = datetime.datetime.now() - start
                log.debug('Query took: %s' % delta)
                raise
        
        delta = datetime.datetime.now() - start
        log.debug('Query took: %s' % delta)
        return results, rowcount
        
    def insert(self, records, lookup, options):
        """
        Inserts the table instance into the database.  If the
        dryRun flag is specified, then the command will be 
        logged but not executed.
        
        :param      records  | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.DatabaseOptions>
        
        :return     <dict> changes
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif orb.Table.recordcheck(records):
            records = [records]
        
        # determine the proper records for insertion
        inserter = defaultdict(list)
        changes = []
        for record in records:
            # make sure we have some data to insert
            rchanges = record.changeset(columns=lookup.columns)
            changes.append(rchanges)
            
            # do not insert records that alread exist
            if options.force:
                pass
            
            elif record.isRecord():
                continue
            
            elif not rchanges:
                continue
            
            inserter[record.schema()].append(record)
        
        cmds = []
        data = {}
        data['output'] = {}
        
        autoinc = options.autoIncrement
        INSERT = self.sql('INSERT')
        INSERTED_KEYS = self.sql('INSERTED_KEYS')
        
        engine = self.engine()
        for schema, schema_records in inserter.items():
            if not schema_records:
                continue
            
            colcount = len(schema.columns())
            batchsize = self.insertBatchSize()
            size = batchsize / max(int(round(colcount/10.0)), 1)
            
            for batch in projex.iters.batch(schema_records, size):
                batch = list(batch)
                icmd, _ = INSERT(schema,
                                 batch,
                                 columns=lookup.columns,
                                 autoincrement=autoinc,
                                 __data__=data)
                cmds.append(icmd)
            
            # for inherited schemas in non-OO tables, we'll define the
            # primary keys before insertion
            if autoinc and (not schema.inherits() or self.isObjectOriented()):
                cmd, _ = INSERTED_KEYS(schema, count=len(batch), __data__=data)
                cmds.append(cmd)
        
        if not cmds:
            return {}
        
        cmd = u'\n'.join(cmds)
        cmd_data = data['output']
        
        if options.dryRun:
            print cmd % cmd_data
            
            if len(changes) == 1:
                return {}
            else:
                return []
        else:
            results, _ = self.execute(cmd, cmd_data, autoCommit=False)
        
        if not self.commit():
            if len(changes) == 1:
                return {}
            return []
        
        # update the values for the database
        for i, record in enumerate(records):
            try:
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
        Returns whether or not this conection is currently
        active.
        
        :return     <bool> connected
        """
        return self.nativeConnection() != None
    
    def nativeConnection(self):
        """
        Returns the sqlite database for the current thread.
        
        :return     <variant> || None
        """
        tid = threading.current_thread().ident
        return self.__threads.get(tid)
    
    def open(self):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :return     <bool> success
        """
        tid = threading.current_thread().ident
        
        # clear out old ids
        for thread in threading.enumerate():
            if not thread.isAlive():
                self.__threads.pop(thread.ident, None)
        
        conn = self.__threads.get(tid)
        
        # check to see if we already have a connection going
        if conn:
            return True
        
        # make sure we have a database assigned to this backend
        elif not self._database:
            raise errors.DatabaseNotFoundError()
        
        # open a new backend connection to the database for this thread
        conn = self._open(self._database)
        if conn:
            self.__threads[tid] = conn
            orb.system.runCallback(orb.CallbackType.ConnectionCreated,
                                       self._database)
        else:
            orb.system.runCallback(orb.CallbackType.ConnectionFailed,
                                       self._database)
        
        return conn != None
    
    def reconnect(self):
        """
        Forces a reconnection to the database.
        """
        tid = threading.current_thread().ident
        db = self.__threads.pop(tid, None)
        if db:
            try:
                db.close()
            except:
                pass
        
        return self.open()
    
    def removeRecords(self, remove, options):
        """
        Removes the inputed record from the database.
        
        :param      remove  | {<orb.Table>: [<orb.Query>, ..], ..}
                    options | <orb.DatabaseOptions>
        
        :return     <int> number of rows removed
        """
        if not remove:
            return 0
        
        # include various schema records to remove
        count = 0
        DELETE = self.sql('DELETE')
        for table, queries in remove.items():
            for query in queries:
                sql, data = DELETE(table, query)
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
    
    def select(self, table_or_join, lookup, options):
        """
        Selects from the database for the inputed items where the
        results match the given dataset information.
        
        :param      table_or_join   | <subclass of orb.Table> || <orb.Join>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.DatabaseOptions>
        
        :return     [({<str> columnName: <variant> value, .., ..), ..]
        """
        COLUMNS = {}
        
        if orb.Table.typecheck(table_or_join):
            schemas = [table_or_join.schema()]
            SELECT = self.sql('SELECT')
            try:
                sql, data = SELECT(table_or_join,
                                   lookup=lookup,
                                   options=options,
                                   COLUMNS=COLUMNS)
            except errors.EmptyQuery:
                return []
        else:
            schemas = [table_or_join.schemas()]
            SELECT_JOIN = self.sql('SELECT_JOIN')
            
            try:
                sql, data = SELECT_JOIN(table_or_join,
                                        lookup=lookup,
                                        toptions=options,
                                        COLUMNS=COLUMNS)
            except errors.EmptyQuery:
                return []
        
        if not sql:
            db_records = []
        else:
            db_records, _ = self.execute(sql, data, autoCommit=False)
        
        # restore the records from the database
        output = []
        is_oo = self.isObjectOriented()
        db = self.database()
        for db_record in db_records:
            records = {}
            for db_key in db_record:
                column = COLUMNS.get(db_key)
                if not column:
                    continue
                
                engine = column.engine(db)
                value  = engine.unwrap(column, db_record[db_key])
                
                schema = column.firstMemberSchema(schemas)
                records.setdefault(schema, {})
                
                _, lang = column.fieldInfo(db_key)
                if lang is not None:
                    records[schema].setdefault(column.name(), {})
                    records[schema][column.name()][lang] = value
                else:
                    records[schema][column.name()] = value
            
            out_records = []
            for schema in schemas:
                out_records.append(records.get(schema))
            output.append(out_records)
        
        if orb.Table.typecheck(table_or_join):
            return map(lambda x: x[0], output)
        return output
    
    def setInsertBatchSize(self, size):
        """
        Sets the maximum number of records that can be inserted for a single
        insert statement.
        
        :param      size | <int>
        """
        self.__insertBatchSize = size
    
    def setRecords(self, schema, records, **options):
        """
        Restores the data for the inputed schema.
        
        :param      schema  | <orb.TableSchema>
                    records | [<dict> record, ..]
        """
        if not records:
            return
        
        engine = self.engine()
        cmds = []
        data = {}
        
        # truncate the table
        cmd, dat = engine.truncateCommand(schema)
        self.execute(cmd, dat, autoCommit=False)
        
        # disable the tables keys
        cmd, dat = engine.disableInternalsCommand(schema)
        self.execute(cmd, dat, autoCommit=False)
        
        colcount = len(schema.columns())
        batchsize = self.insertBatchSize()
        size = batchsize / max(int(round(colcount/10.0)), 1)
        
        # insert the records
        cmds  = []
        dat   = {}
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
    
    def tableExists(self, schema, options):
        """
        Checks to see if the inputed table class exists in the
        database or not.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.DatabaseOptions>
        
        :return     <bool> exists
        """
        TABLE_EXISTS = self.sql('TABLE_EXISTS')
        sql, data = TABLE_EXISTS(schema)
        return bool(self.execute(sql, data, autoCommit=False)[0])
    
    def update(self, records, lookup, options):
        """
        Updates the modified data in the database for the 
        inputed record.  If the dryRun flag is specified then
        the command will be logged but not executed.
        
        :param      record   | <orb.Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.DatabaseOptions>
        
        :return     <dict> changes
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif orb.Table.recordcheck(records):
            records = [records]
        
        is_oo = self.isObjectOriented()
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
            
            if not is_oo:
                schemas = record.schema().ancestry() + [record.schema()]
            else:
                schemas = [record.schema()]
            
            for schema in schemas:
                cols = map(schema.column, rchanges.keys())
                updater[schema].append((record, cols))
        
        if not updater:
            if len(records) > 1:
                return []
            else:
                return {}
        
        cmds = []
        data = {}
        
        UPDATE = self.sql('UPDATE')
        
        for schema, changes in updater.items():
            icmd, _ = UPDATE(schema, changes, __data__=data)
            cmds.append(icmd)
        
        cmd = u'\n'.join(cmds)
        
        if options.dryRun:
            print cmd % data
            if len(changes) == 1:
                return {}
            else:
                return []
        else:
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
    
    def updateTable(self, schema, options):
        """
        Determines the difference between the inputed schema
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
                    options    | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        # determine the new columns
        existing = self.existingColumns(schema, options)
        missing  = schema.fieldNames(recurse=False,
                                     includeProxies=False,
                                     includeJoined=False,
                                     includeAggregates=False,
                                     ignore=existing)
        
        # if no columns are missing, return True to indicate the table is
        # up to date
        if not missing:
            return True
        
        columns = map(schema.column, missing)
        ALTER = self.sql('ALTER_TABLE')
        sql, data = ALTER(schema.model(), added=columns)
        
        if options.dryRun:
            print sql % data
        else:
            log.info('Updating {0}...'.format(schema.name()))
            
            self.execute(sql, data)
            
            opts = (schema.name(), ','.join(missing))
            log.info('Updated {0} table: added {1}'.format(*opts))
        
        return True

    @classmethod
    def sql(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :return     subclass of <orb.backends.sql.SQL>
        """
        if code:
            return SQL.byName(code)
        else:
            return SQL
