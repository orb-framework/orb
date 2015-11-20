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
        self.__database = database

    def __del__(self):
        """
        Closes the connection when the connection instance is deleted.
        """
        self.close()

    @abstractmethod
    def _delete(self, records, context):
        """
        Removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.

        :param      records  | {<orb.Table>: [<orb.Query>, ..], ..}
                    context  | <orb.Context>

        :return     <int> | number of rows removed
        """
        return 0

    @abstractmethod
    def alterModel(self, model, context, add=None, remove=None, owner=''):
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
                    options  | <orb.Context>

        :return     <bool> success
        """
        return False

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

    def collectDeleteRecords(self, table, lookup, options, collection=None):
        """
        Collects records for removal by looking up the schema's relations
        in reference to the given records.  If there are records that should
        not be removed, then the CannotRemoveError will be raised.
        
        :param      table      | <orb.Table>
                    lookup     | <orb.LookupOptions>
                    options    | <orb.Context>
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
                            ref_query |= orb.Query(column.name()).between(min_key,
                                                                          max_key)

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
                    self.collectDeleteRecords(ref_table,
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

    @abstractmethod()
    def count(self, table_or_join, lookup, options):
        """
        Returns the number of records that exist for this connection for
        a given lookup and options.
        
        :sa         distinct, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.Context>
        
        :return     <int>
        """
        return 0

    @abstractmethod()
    def createModel(self, model, context, owner='', includeReferences=True):
        """
        Creates a new table in the database based cff the inputted
        table information.
        
        :param      schema   | <orb.TableSchema>
                    options  | <orb.Context>
        
        :return     <bool> success
        """
        return False

    def database(self):
        """
        Returns the database instance that this connection is
        connected to.
        
        :return     <Database>
        """
        return self.__database

    def delete(self, records, context):
        """
        Removes the given records from the inputted schema.  This method is
        called from the <Connection.remove> method that handles the pre
        processing of grouping records together by schema and only works
        on the primary key.

        :param      table     | <subclass of orb.Table>
                    context   | <orb.Context>

        :return     <int> | number of rows removed
        """
        with orb.Transaction():
            return self._delete(records, context)

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

    @abstractmethod()
    def distinct(self, table_or_join, lookup, options):
        """
        Returns the distinct set of records that exist for a given lookup
        for the inputted table or join instance.
        
        :sa         count, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.Context>
        
        :return     {<str> columnName: <list> value, ..}
        """
        return 0

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
    def insert(self, records, context):
        """
        Inserts the database record into the database with the
        given values.
        
        :param      records     | <orb.Table>
                    lookup      | <orb.LookupOptions>
                    options     | <orb.Context>
        
        :return     <bool>
        """
        return False

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

    @abstractmethod()
    def rollback(self):
        """
        Rolls back the latest code run on the database.
        """
        return False

    @abstractmethod()
    def select(self, model, context):
        """
        Selects the records from the database for the inputted table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.Context>
        
        :return     [<variant> result, ..]
        """
        return []

    def setup(self, context):
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

    @abstractmethod()
    def schemaInfo(self, options):
        """
        Returns the schema information from the database.

        :return     <dict>
        """
        return {}

    @abstractmethod()
    def update(self, records, context):
        """
        Updates the database record into the database with the
        given values.
        
        :param      record  | <orb.Table>
                    options | <orb.Context>
        
        :return     <bool>
        """
        return False

