"""
Defines the base connection class that will be used for communication
to the backend databases.
"""

import cPickle
import logging
import projex.iters

from collections import OrderedDict
from projex.addon import AddonManager
from projex.decorators import abstractmethod
from projex.lazymodule import lazy_import

orb = lazy_import('orb')
errors = lazy_import('orb.errors')

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
        self._database = database
        self._threadEnabled = False
        self._internalsEnabled = True
        self._commitEnabled = True
        self._engine = None
        self._columnEngines = {}

    def __del__(self):
        """
        Closes the connection when the connection instance is deleted.
        """
        self.close()

    # noinspection PyUnusedLocal
    def backup(self, filename, **options):
        """
        Backs up the data from this database backend to the inputted filename.
        
        :param      filename | <str>
        
        :return     <bool> | success
        """
        db = self.database()
        data = {}
        for schema in self.database().schemas():
            colnames = schema.columnNames(orb.Column.Flags.Field)
            values = schema.model().select(colnames,
                                           inflated=False,
                                           db=db).records()
            data[schema.name()] = [dict(zip(colnames, x)) for x in values]

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
        Closes the connection to the database for this connection.
        
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
                    options    | <orb.ContextOptions>
                    collection | <dictionary> | input/output variable
        
        :return     {<orb.Table>: [<orb.Query>, ..], ..} | cascaded
        """
        if collection is None:
            collection = OrderedDict()

        # define the base lookup query
        query = lookup.where

        # lookup cascading removal
        def load_keys(t, q):
            return t.select(where=q).primaryKeys()

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
                            between_q = orb.Query(column.name()) >= min_key
                            between_q &= orb.Query(column.name()) <= max_key
                            ref_query |= between_q

                # remove if there is a valid reference
                else:
                    # noinspection PyComparisonWithNone,PyPep8
                    ref_query = orb.Query(column.name()) != None

                # block removal of records
                if flags & orb.DeleteFlags.Blocked and action == orb.RemovedAction.Block:
                    found = ref_table.select(where=ref_query)
                    if not found.isEmpty():
                        msg = 'Could not remove records, there are still ' \
                              'references to the {0} model.'.format(ref_table.__name__)
                        raise errors.CannotDelete(msg)

                # cascade additional removals
                elif flags & orb.DeleteFlags.Cascaded and action == orb.RemovedAction.Cascade:
                    ref_lookup = orb.LookupOptions(where=ref_query)
                    self.collectRemoveRecords(ref_table,
                                              ref_lookup,
                                              options,
                                              collection)

        if query is None or not query.isNull():
            collection.setdefault(table, [])
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
                    options       | <orb.ContextOptions>
        
        :return     <int>
        """
        return 0

    @abstractmethod()
    def createTable(self, schema, options):
        """
        Creates a new table in the database based cff the inputted
        table information.
        
        :param      schema   | <orb.TableSchema>
                    options  | <orb.ContextOptions>
        
        :return     <bool> success
        """
        return False

    @abstractmethod()
    def createView(self, view, options):
        """
        Creates a new view in the database based off the inputted schema information.

        :param      schema  | <orb.ViewSchema>
                    options | <orb.ContextOptions>

        :return     <bool> | success
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
                          settings.primaryName(),
                          primary=True,
                          autoIncrement=True,
                          fieldName=settings.primaryField(),
                          getterName=settings.primaryGetter(),
                          setterName=settings.primarySetter(),
                          displayName=settings.primaryDisplay(),
                          indexName=settings.primaryIndex(),
                          indexed=True,
                          unique=True,
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
    def distinct(self, table_or_join, lookup, options):
        """
        Returns the distinct set of records that exist for a given lookup
        for the inputted table or join instance.
        
        :sa         count, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.ContextOptions>
        
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
    def execute(self, command, data=None, flags=0):
        """
        Executes the inputted command into the current
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
                    options     | <orb.ContextOptions>
        
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
    def isConnected(self):
        """
        Returns whether or not this connection is currently
        active.
        
        :return     <bool> connected
        """
        return False

    def isThreadEnabled(self):
        """
        Returns whether or not this connection can be threaded.
        
        :return     <bool>
        """
        return self._threadEnabled

    @abstractmethod()
    def open(self, force=False):
        """
        Opens a new database connection to the database defined
        by the inputted database.  If the force parameter is provided, then
        it will re-open a connection regardless if one is open already

        :param      force | <bool>

        :return     <bool> success
        """
        return False

    def remove(self, table, lookup, options):
        """
        Removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.
        
        :param      table     | <subclass of orb.Table>
                    lookup    | <orb.LookupOptions>
                    options   | <orb.ContextOptions>
        
        :return     <int> | number of rows removed
        """
        removals = self.collectRemoveRecords(table, lookup, options)
        with orb.Transaction():
            count = self.removeRecords(removals, options)

        # mark the tables caches expired
        for rem_table in removals:
            rem_table.markTableCacheExpired()

        return count

    @abstractmethod
    def removeRecords(self, remove, options):
        """
        Removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.
        
        :param      remove  | {<orb.Table>: [<orb.Query>, ..], ..}
                    options | <orb.ContextOptions>
        
        :return     <int> | number of rows removed
        """
        return 0

    def restore(self, filename, **options):
        """
        Restores this backend database from the inputted pickle file.
        
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
            print '{0:.0%} complete: restoring {1}...'.format(i / count,
                                                              schema_name)

            # determine if this schema should be ignored
            if schema_name in ignore:
                print 'ignoring {0}...'.format(schema_name)
                continue

            elif include and schema_name not in include:
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
    def rollback(self):
        """
        Rolls back the latest code run on the database.
        """
        return False

    @abstractmethod()
    def select(self, table_or_join, lookup, options):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.ContextOptions>
        
        :return     [<variant> result, ..]
        """
        return []

    def selectFirst(self, table_or_join, lookup, options):
        """
        Returns the first result based on the inputted query options \
        from the database.database.
        
        
        :param      table    | <subclass of Table>
                    lookup   | <orb.LookupOptions>
                    options  | <orb.ContextOptions>
        
        :return     <Table> || None
        """
        # limit the lookup information to 1
        lookup.limit = 1

        # load the results
        results = self.select(table_or_join, lookup, options)

        if results:
            return results[0]
        return None

    def setupDatabase(self, options):
        """
        Initializes the database with any additional information that is required.
        """
        pass

    @abstractmethod()
    def setRecords(self, schema, records):
        """
        Restores the data for the inputted schema.
        
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

    @abstractmethod()
    def schemaInfo(self, options):
        """
        Returns the schema information from the database.

        :return     <dict>
        """
        return {}

    def storeRecord(self, record, lookup, options):
        """
        Syncs the record to the current database, checking to \
        see if the record exists, and if so - updates the records \
        field values, otherwise, creates the new record.  The possible sync \
        return types are 'created', 'updated', and 'errored'.
        
        :param      record      | <orb.Table>
                    lookup      | <orb.LookupOptions>
                    options     | <orb.ContextOptions>
        
        :return     <variant>
        """
        # create the new record in the database
        if not record.isRecord():
            return self.insert(record, lookup, options)
        else:
            return self.update(record, lookup, options)

    def syncTable(self, schema, options):
        """
        Syncs the table to the current database, checking to see
        if the table exists, and if so - updates any new columns,
        otherwise, creates the new table.
        
        :param      schema     | <orb.TableSchema>
                    options    | <orb.ContextOptions>
        
        :return     <bool> changed
        """
        if not self.tableExists(schema, options):
            return self.createTable(schema, options)
        else:
            return self.updateTable(schema, options)

    @abstractmethod()
    def tableExists(self, schema, options):
        """
        Checks to see if the inputted table class exists as a
        database table.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.ContextOptions>
        
        :return     <bool>
        """
        return False

    @abstractmethod()
    def update(self, records, lookup, options):
        """
        Updates the database record into the database with the
        given values.
        
        :param      record  | <orb.Table>
                    options | <orb.ContextOptions>
        
        :return     <bool>
        """
        return False

    @abstractmethod
    def updateTable(self, schema, info, options):
        """
        Determines the difference between the inputted table
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
                    options  | <orb.ContextOptions>
        
        :return     <bool> success
        """
        return False

    @staticmethod
    def create(database):
        """
        Returns a new database connection for the inputted database
        from the registered backends and the database type
        
        :return     <Connection> || None
        """
        cls = Connection.byName(database.databaseType())
        if not cls:
            raise errors.BackendNotFound(database.databaseType())
        return cls(database)


# register the addon module
from orb.core.backends import __plugins__

Connection.registerAddonModule(__plugins__)

