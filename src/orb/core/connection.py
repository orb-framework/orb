#!/usr/bin/python

"""
Defines the base connection class that will be used for communication
to the backend databases.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import cPickle
import logging
import projex.iters

from projex.addon import AddonManager
from projex.decorators import abstractmethod
from projex.lazymodule import LazyModule

orb = LazyModule('orb')
errors = LazyModule('orb.errros')

log = logging.getLogger(__name__)


class Connection(AddonManager):
    """ 
    Defines the base connection class type.  This class is used to handle
    database transactions and queries.  The Connection class needs to be
    subclassed to handle connection to a particular kind of database backend,
    the backends that are included with the orb package can be found in the
    <orb.backends> package.
    """
    def __init__(self, database):
        super(Connection, self).__init__()
        
        # define custom properties
        self._database          = database
        self._threadEnabled     = False
        self._internalsEnabled  = True
        self._commitEnabled     = True
        self._engine            = None
        self._columnEngines     = {}
    
    def __del__(self):
        """
        Closes the connection when the connection instance is deleted.
        """
        self.close()
    
    def backup(self, filename, **options):
        """
        Backs up the data from this database backend to the inputed filename.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        db = self.database()
        data = {}
        for schema in self.database().schemas():
            colnames = schema.columnNames(includeProxies=False,
                                          includeJoined=False,
                                          includeAggregates=False)
            
            values = schema.model().select(colnames,
                                           inflated=False,
                                           db=db).all()
            data[schema.name()] = map(lambda x: dict(zip(colnames, x)), values)
        
        # save this backup to the database file
        f = open(filename, 'wb')
        cPickle.dump(data, f)
        f.close()

    @abstractmethod()
    def cleanup(self):
        """
        Cleans up the database for any unused memory or information.
        """
        pass

    @abstractmethod()
    def close(self):
        """
        Closes the connection to the datbaase for this connection.
        
        :return     <bool> closed
        """
        return True
    
    def collectRemoveRecords(self, table, lookup, options, collection=None):
        """
        Collects records for removal by looking up the schema's relations
        in reference to the given records.  If there are records that should
        not be removed, then the CannotRemoveError will be raised.
        
        :param      table      | <orb.Table>
                    lookup     | <orb.LookupOptions>
                    options    | <orb.DatabaseOptions>
                    collection | <dictionary> | input/output variable
        
        :return     {<orb.Table>: [<orb.Query>, ..], ..} | cascaded
        """
        if collection is None:
            collection = {}
        
        # define the base lookup query
        query = lookup.where
        if query is None:
            query = orb.Query()
        
        # add the base collection data
        collection.setdefault(table, [])
        
        # lookup cascading removal
        def load_keys(table, query):
            return table.select(where=query).primaryKeys()
        root_keys = None
        
        # lookup related records
        flags = options.deleteFlags
        if flags is None:
            flags = orb.DeleteFlags.all()
        
        relations = orb.Orb.instance().findRelations(table.schema())
        for ref_table, columns in relations:
            for column in columns:
                action = column.referenceRemovedAction()
                
                # remove based on a certain criteria
                if query is not None:
                    if root_keys is None:
                        root_keys = load_keys(table, query)
                    
                    ref_query = orb.Query()
                    for min_key, max_key in projex.iters.group(root_keys):
                        if min_key == max_key:
                            ref_query |= orb.Query(column.name()) == min_key
                        else:
                            ref_query |= orb.Query(column.name()).between(min_key,
                                                                  max_key)
                
                # remove if there is a valid reference
                else:
                    ref_query = orb.Query(column.name()) != None
                
                # block removal of records
                if flags & orb.DeleteFlags.Blocked and \
                   action == orb.RemovedAction.Block:
                    found = ref_table.select(where=ref_query)
                    if not found.isEmpty():
                        msg = 'Could not remove records, there are still '\
                              'references to the %s model.'
                        tblname = ref_table.__name__
                        raise errors.CannotRemoveError(msg, tblname)
                
                # cascade additional removals
                elif flags & orb.DeleteFlags.Cascaded and \
                     action == orb.RemovedAction.Cascade:
                    ref_lookup = orb.LookupOptions(where=ref_query)
                    self.collectRemoveRecords(ref_table,
                                              ref_lookup,
                                              options,
                                              collection)
        
        if query:
            collection[table].append(query)
        
        return collection
    
    @abstractmethod()
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return     <bool> success
        """
        return False
    
    def commitEnabled(self):
        """
        Returns whether or not committing is currently enabled.
        
        :return     <bool>
        """
        return self._commitEnabled and self.internalsEnabled()
    
    @abstractmethod()
    def count(self, table_or_join, lookup, options):
        """
        Returns the number of records that exist for this connection for
        a given lookup and options.
        
        :sa         distinct, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     <int>
        """
        return 0
    
    @abstractmethod()
    def createTable( self, schema, options ):
        """
        Creates a new table in the database based cff the inputed
        table information.
        
        :param      schema   | <orb.TableSchema>
                    options  | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        return False
    
    def columnEngine(self, columnType):
        """
        Returns the data engine associated with this backend for the given
        column type.
        
        :param      columnType | <orb.ColumnType>
        """
        return self._columnEngines.get(columnType)
    
    def columnEngines(self):
        """
        Returns a dict of the column engines associated with this connection.
        
        :return     {<orb.ColumnType> coltype: <orb.ColumnEngine>, ..}
        """
        return self._columnEngines
    
    def database(self):
        """
        Returns the database instance that this connection is
        connected to.
        
        :return     <Database>
        """
        return self._database
    
    def defaultPrimaryColumn(self):
        """
        Defines a default column to be used as the primary column for this \
        database connection.  By default, an auto-incrementing integer field \
        called '_id' will be defined.
        
        :return     <orb.Column>
        """
        settings = orb.system.settings()
        return orb.Column(orb.ColumnType.Integer,
                          settings.primaryField(),
                          primary=True,
                          autoIncrement=True,
                          fieldName=settings.primaryField(),
                          getterName=settings.primaryGetter(),
                          setterName=settings.primarySetter(),
                          displayName=settings.primaryDisplay(),
                          indexName=settings.primaryIndex(),
                          indexed=True,
                          unique=True,
                          private=True,
                          searchable=True)
    
    def defaultInheritColumn(self, schema):
        """
        Defines a default column to be used as the primary column for this \
        database connection.  By default, an auto-incrementing integer field \
        called '_id' will be defined.
        
        :return     <orb.Column>
        """
        settings = orb.system.settings()
        col = orb.Column(orb.ColumnType.ForeignKey,
                         settings.inheritField(),
                         fieldName=settings.inheritField(),
                         unique=True,
                         private=True)
        
        col.setReference(schema.inherits())
        col._schema = schema
        
        return col
    
    def disableInternals(self):
        """
        Disables the internal checks and update system.  This method should
        be used at your own risk, as it will ignore errors and internal checks
        like auto-incrementation.  This should be used in conjunction with
        the enableInternals method, usually these are used when doing a
        bulk import of data.
        
        :sa     enableInternals
        """
        self._internalsEnabled = False
    
    @abstractmethod()
    def distinct( self, table_or_join, lookup, options ):
        """
        Returns the distinct set of records that exist for a given lookup
        for the inputed table or join instance.
        
        :sa         count, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     {<str> columnName: <list> value, ..}
        """
        return 0
        
    def enableInternals(self):
        """
        Enables the internal checks and update system.  This method should
        be used at your own risk, as it will ignore errors and internal checks
        like auto-incrementation.  This should be used in conjunction with
        the disableInternals method, usually these are used when doing a
        bulk import of data.
        
        :sa     disableInternals
        """
        self._internalsEnabled = True
        
    def engine(self):
        """
        Returns the engine associated with this connection backend.
        
        :return     <orb.SchemaEngine>
        """
        return self._engine
    
    @abstractmethod()
    def execute( self, command, data=None, flags=0):
        """
        Executes the inputed command into the current 
        connection cursor.
        
        :param      command  | <str>
                    data     | <dict> || None
                    flags    | <orb.DatabaseFlags>
        
        :return     <variant> returns a native set of information
        """
        return None
    
    @abstractmethod()
    def insert(self, records, lookup, options):
        """
        Inserts the database record into the database with the
        given values.
        
        :param      records     | <orb.Table>
                    lookup      | <orb.LookupOptions>
                    options     | <orb.DatabaseOptions>
        
        :return     <bool>
        """
        return False
    
    def internalsEnabled(self):
        """
        Returns whether or not this connection has its internal checks and
        optimizations enabled or not.
        
        :sa         disableInternals, enableInternals
        
        :return     <bool>
        """
        return self._internalsEnabled
    
    def interrupt(self, threadId=None):
        """
        Interrupts/stops the database access through a particular thread.
        
        :param      threadId | <int> || None
        """
        pass
    
    @abstractmethod()
    def isConnected( self ):
        """
        Returns whether or not this conection is currently
        active.
        
        :return     <bool> connected
        """
        return False
    
    def isThreadEnabled( self ):
        """
        Returns whether or not this connection can be threaded.
        
        :return     <bool>
        """
        return self._threadEnabled
    
    @abstractmethod()
    def open( self ):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :return     <bool> success
        """
        return False
    
    def remove(self, table, lookup, options):
        """
        Removes the given records from the inputed schema.  This method is 
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.
        
        :param      table     | <subclass of orb.Table>
                    lookup    | <orb.LookupOptions>
                    options   | <orb.DatabaseOptions>
        
        :return     <int> | number of rows removed
        """
        removals = self.collectRemoveRecords(table, lookup, options)
        count = self.removeRecords(removals, options)
        for rem_table in removals:
            rem_table.markTableCacheExpired()
        return count
    
    @abstractmethod
    def removeRecords(self, remove, options):
        """
        Removes the given records from the inputed schema.  This method is 
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.
        
        :param      remove  | {<orb.Table>: [<orb.Query>, ..], ..}
                    options | <orb.DatabaseOptions>
        
        :return     <int> | number of rows removed
        """
        return 0
    
    def restore(self, filename, **options):
        """
        Restores this backend database from the inputed pickle file.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        # save this backup to the database file
        print '0% complete: loading data dump...'
        
        f = open(filename, 'rb')
        data = cPickle.load(f)
        f.close()
        
        items = data.items()
        count = float(len(items))
        db_name = self.database().name()
        i = 0
        
        ignore = options.get('ignore', [])
        include = options.get('include', [])
        
        self.disableInternals()
        options.setdefault('autoCommit', False)
        for schema_name, records in items:
            i += 1
            print '{0:.0%} complete: restoring {1}...'.format(i/count,
                                                              schema_name)
            
            # determine if this schema should be ignored
            if schema_name in ignore:
                print 'ignoring {0}...'.format(schema_name)
                continue
            
            elif include and not schema_name in include:
                print 'ignoring {0}...'.format(schema_name)
                continue
            
            schema = orb.Orb.instance().schema(schema_name,
                                               database=db_name)
            if schema:
                self.setRecords(schema, records, **options)
            else:
                print schema_name, 'not found'
        
        self.enableInternals()
        self.commit()
        
        return True
    
    @abstractmethod()
    def rollback( self ):
        """
        Rollsback the latest code run on the database.
        """
        return False
    
    @abstractmethod()
    def select(self, table_or_join, lookup, options):
        """
        Selects the records from the database for the inputed table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.DatabaseOptions>
        
        :return     [<variant> result, ..]
        """
        return []
    
    def selectFirst(self, table_or_join, lookup, options):
        """
        Returns the first result based on the inputed query options \
        from the database.database.
        
        
        :param      table    | <subclass of Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.DatabaseOptions>
        
        :return     <Table> || None
        """
        # limit the lookup information to 1
        lookup.limit = 1
        
        # load the results
        results = self.select( table, query_options, options )
        
        if ( results ):
            return results[0]
        return None
    
    @abstractmethod()
    def setRecords(self, schema, records):
        """
        Restores the data for the inputed schema.
        
        :param      schema  | <orb.TableSchema>
                    records | [<dict> record, ..]
        """
        pass
    
    def setCommitEnabled(self, state):
        """
        Sets whether or not committing changes to the database is currently
        enabled.
        
        :param      state | <bool>
        """
        self._commitEnabled = state
    
    def setColumnEngine(self, columnType, engine):
        """
        Sets the data engine associated with this backend for the given
        column type.
        
        :param      columnType | <orb.ColumnType>
                    engine     | <orb.ColumnEngine>
        """
        self._columnEngines[columnType] = engine
    
    def setEngine(self, engine):
        """
        Returns the table engine associated with this connection backend.
        
        :param     engine | <orb.SchemaEngine>
        """
        self._engine = engine
    
    def setThreadEnabled(self, state):
        """
        Sets whether or not this database backend supports threading.
        
        :param      state | <bool>
        """
        self._threadEnabled = state
    
    def syncRecord(self, record, lookup, options):
        """
        Syncs the record to the current database, checking to \
        see if the record exists, and if so - updates the records \
        field values, otherise, creates the new record.  The possible sync \
        return types are 'created', 'updated', and 'errored'.
        
        :param      record      | <orb.Table>
                    lookup      | <orb.LookupOptions>
                    options     | <orb.DatabaseOptions>
        
        :return     (<str> type, <dict> changeet) || None
        """
        db = self.database()
        changes = record.changeset()
        if not changes:
            return ('', [])
        
        # create the new record in the database
        if not record.isRecord():
            results = self.insert(record, lookup, options)
            if 'db_error' in results:
                return ('errored', results )
            return ('created', results)
        else:
            results = self.update(record, lookup, options)
            if 'db_error' in results:
                return ('errored', results)
            return ('updated', results)
    
    def syncTable(self, schema, options):
        """
        Syncs the table to the current database, checking to see
        if the table exists, and if so - updates any new columns,
        otherwise, creates the new table.
        
        :param      schema     | <orb.TableSchema>
                    options    | <orb.DatabaseOptions>
        
        :return     (<str> type, <bool> changed) || None
        """
        if self.tableExists(schema, options):
            results = self.updateTable(schema, options)
            return ('created', )
        else:
            results = self.createTable(schema, options)
            return ('updated', results)
    
    @abstractmethod()
    def tableExists(self, schema, options):
        """
        Checks to see if the inputed table class exists as a
        database table.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.DatabaseOptions>
        
        :return     <bool>
        """
        return False
    
    @abstractmethod()
    def update(self, record, options):
        """
        Updates the database record into the database with the
        given values.
        
        :param      record  | <orb.Table>
                    options | <orb.DatabaseOptions>
        
        :return     <bool>
        """
        return False
    
    @abstractmethod
    def updateTable(self, table, options):
        """
        Determines the difference between the inputed table
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
        
        :param      table    | <orb.TableSchema>
                    options  | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        return False
    
    @staticmethod
    def create(database):
        """
        Returns a new datbase connection for the inputed database
        from the registered backends and the database type
        
        :return     <Connection> || None
        """
        cls = Connection.byName(database.databaseType())
        if not cls:
            from orb import errors
            raise errors.BackendNotFoundError(database.databaseType())
        return cls(database)


# register the addon module
from orb.backends import __plugins__
Connection.registerAddonModule(__plugins__)
