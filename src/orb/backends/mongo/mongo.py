#!/usr/bin/python

""" Defines the backend connection class for Mongo databases. """

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
__depends__        = ['pymongo', 'bson']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__

#------------------------------------------------------------------------------

import datetime
import logging
import orb
import projex.text
import re

from orb import Query as Q
from orb import errors

logger = logging.getLogger(__name__)

try:
    import pymongo
    import bson
    
except ImportError:
    logger.debug('For Mongo backend, download the pymongo module')
    
    pymongo = None
    bson    = None

#------------------------------------------------------------------------------

class Mongo(orb.Connection):
    """ 
    Creates a Mongo backend connection type for handling database \
    connections to Mongo databases.
    """
    
    # map the default operator types to a SQL operator
    OpMap = {
        Q.Op.Is:                    '=',
        Q.Op.IsNot:                 '!=',
        Q.Op.LessThan:              '<',
        Q.Op.LessThanOrEqual:       '<=',
        Q.Op.GreaterThan:           '>',
        Q.Op.GreaterThanOrEqual:    '>=',
    }
    
    def __init__(self, database):
        super(Mongo, self).__init__(database)
        
        # define custom properties
        self._connection = None
        self._mongodb    = None
        self._failed     = False
        
        # set standard properties
        self.setThreadEnabled(True)
    
    def _select(self, table_or_join, lookup, options):
        """
        Performs the database lookup and returns the raw pymongo information
        for processing.
        
        :param      table_or_join | <subclass of orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     <variant>
        """
        db = self.mongodb()
        if not db:
            return []
        
        schemas = []
        
        # initialize a lookup from a table
        if orb.Table.typecheck(table_or_join):
            schemas.append((table_or_join.schema(), lookup.columns))
            
        # intialize a lookup from a join
        elif orb.Join.typecheck(table_or_join):
            logger.warning('Joining is not yet supported for MongoDB')
            return []
        
        # make sure we have a valid query
        else:
            raise errors.QueryInvalid('Invalid select parameter: {0}'.format(table_or_join))
        
        limit = 0
        start = 0
        
        if lookup.limit:
            limit = lookup.limit
        if lookup.start:
            start = lookup.start
        
        output = []
        for schema, columns in schemas:
            collection = db[schema.tableName()]
            
            specs       = None
            fields      = None
            sort        = None
            
            # collect specific columns
            if lookup.columns is not None:
                specs   = {}
                columns = [schema.column(c).fieldName() for c in lookup.columns]
                fields  = dict(map(lambda x: (x, 1), columns))
            
            # collect specific entries
            if lookup.where:
                where = lookup.where.expandShortcuts(table_or_join)
                specs = self.queryCommand(schema, where)
            
            # convert the sorting keys
            if not lookup.order and orb.Table.typecheck(table_or_join):
                lookup.order = table_or_join.schema().defaultOrder()
            
            if lookup.order:
                sort = []
                for col, direction in lookup.order:
                    if ( direction == 'asc' ):
                        direc = pymongo.ASCENDING
                    else:
                        direc = pymongo.DESCENDING
                    
                    sort.append((schema.column(col).fieldName(), direc))
            
            # collect the records
            results = collection.find(spec      = specs, 
                                      fields    = fields,
                                      skip      = start,
                                      limit     = limit,
                                      sort      = sort)
            
            output.append((schema, results))
        return output
    
    def cleanValues(self, schema, data):
        """
        Since pymongo cannot save/restore date values, we have to convert the \
        datetimes for the inputed data back to dates.
        
        :param      schema | <TableSchema>
                    data   | <dict>
        """
        for key, value in data.items():
            if isinstance(value, datetime.datetime):
                
                col = schema.column(key)
                if ( not (col and col.columnType() == orb.ColumnType.Date) ):
                    continue
                
                data[key] = value.date()
            
            elif isinstance(value, unicode):
                col = schema.column(key)
                if ( not (col and col.columnType() in [orb.ColumnType.String, 
                                                       orb.ColumnType.Password,
                                                       orb.ColumnType.Email])):
                    continue
                
                data[key] = projex.text.decoded(value)
            
            elif isinstance(value, bson.objectid.ObjectId):
                data[key] = projex.text.decoded(value)
    
    def close(self):
        """
        Closes the connection to the datbaase for this connection.
        
        :return     <bool> closed
        """
        if not self.isConnected():
            return False
        
        self._connection.close()
        self._connection    = None
        self._mongodb       = None
        
        return True
    
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
        output = 0
        for schema, results in self._select(table_or_join, lookup, options):
            output += results.count()
        return output
    
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return     <bool> success
        """
        if not self.isConnected():
            return False
        
        if orb.Transaction.current():
            orb.Transaction.current().setDirty(self)
        else:
            self._connection.commit()
        return True
    
    def createTable(self, schema, options):
        """
        Creates a new table in the database based cff the inputed
        schema information.  If the dryRun flag is specified, then
        the SQL will only be logged to the current logger, and not
        actually executed in the database.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        if schema.isAbstract():
            name = schema.name()
            logger.debug('%s is an abstract table, not creating' % name)
            return False
            
        tableName   = schema.tableName()
        
        db = self.mongodb()
        if not db:
            return False
        
        if options.dryRun:
            logger.info('Creating collection: "%s".' % tableName)
            return True
        
        logger.debug('Creating collection: "%s".' % tableName)
        
        # create the new collection
        try:
            db.create_collection(tableName)
            return True
        
        # error raised when collection already exists
        except pymongo.errors.CollectionInvalid:
            return False
    
    def defaultPrimaryColumn(self):
        """
        Defines a default column to be used as the primary column for this
        database connection.  By default, an auto-incrementing integer field
        called '_id' will be defined.
        
        :return     <orb.Column>
        """
        return orb.Column(orb.ColumnType.String,
                          'id',
                           primary=True,
                           autoIncrement=True,
                           fieldName='_id',
                           getterName='id',
                           setterName='setId',
                           displayName='Id',
                           indexName='byId',
                           indexed=True,
                           unique=True)
    
    def distinct(self, table_or_join, lookup, options):
        """
        Returns the distinct set of records that exist for a given lookup
        for the inputed table or join instance.
        
        :sa         count, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     {<str> columnName: <list> value, ..}
        """
        columns = lookup.columns
        if not columns:
            return []
        
        output = {}
        for schema, results in self._select(table_or_join, lookup, options):
            for column_name in columns:
                column = schema.column(column_name)
                if ( column ):
                    field = column.fieldName()
                else:
                    field = column_name
                
                output[column_name] = results.distinct(field)
            
            self.cleanValues(schema, output)
            
        return output
    
    def insert(self, record, options):
        """
        Inserts the table instance into the database.  If the
        dryRun flag is specified, then the command will be 
        logged but not executed.
        
        :param      record      | <orb.Table>
                    options     | <orb.DatabaseOptions>
        
        :return     <dict> changes
        """
        schema      = record.schema()
        tableName   = schema.tableName()
        
        db = self.mongodb()
        if not db:
            return {}
            
        changes     = {}
        values      = {}
        
        # insert the columns 
        for column in schema.columns(includeProxies=False):
            columnName  = column.name()
            value       = record.recordValue(columnName)
            fvalue      = value
            
            # ignore auto incrementing columns
            if column.autoIncrement():
                continue
            
            # make sure all the required columns have been set
            elif column.required() and value == None:
                raise errors.ColumnRequired(columnName)
            
            # no need fo undefined items to be set
            elif value == None:
                continue
            
            # extract the primary key information
            elif orb.Table.recordcheck(value):
                fvalue = value.primaryKey()
                if not fvalue:
                    raise errors.PrimaryKeyNotDefined(value)
                
                if ( isinstance(value.database().backend(), Mongo) ):
                    value = bson.objectid.ObjectId(fvalue)
                else:
                    value = fvalue
            
            changes[columnName] = (None, value)
            
            # using isinstance(x, datetime.date) returns true for datetime types
            if type(value) == datetime.date:
                value = datetime.datetime(value.year,
                                          value.month,
                                          value.day,
                                          0,
                                          0,
                                          0)
            
            values[column.fieldName()]  = value
            
        if not changes:
            return {}
        
        if options.dryRun:
            logger.info( 'dryrun: inserting into db: %s' % values )
        else:
            safe   = options.flags & orb.DatabaseFlags.SafeInsert != 0
            result = db[tableName].insert(values, safe = safe)
            record._updateFromDatabase({'_id': projex.text.decoded(result)})
        
        return changes
    
    def isConnected(self):
        """
        Returns whether or not this conection is currently
        active.
        
        :return     <bool> connected
        """
        return self._connection != None and self._mongodb != None
    
    def mongodb(self):
        """
        Returns the mongo database instance for this connection.
        
        :return     <pymongo.Database> || None
        """
        if not self._mongodb:
            self.open()
            
        return self._mongodb
    
    def open(self):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :return     <bool> success
        """
        # make sure we have a postgres module
        if not pymongo:
            return False
        
        if self._failed:
            return False
        
        # check to see if we already have a connection going
        if self._connection:
            return True
        
        elif not self._database:
            self._failed = Truew
            raise errors.DatabaseNotFound()
        
        dbname  = self._database.databaseName()
        user    = self._database.username()
        pword   = self._database.password()
        host    = self._database.host()
        port    = self._database.port()
        
        # create the python connection
        connection    = pymongo.Connection(host = host, port = port)
        if not connection:
            msg = 'Failed to connect: %s, %s' % (host, port)
            logger.error(errors.DatabaseError(msg))
            self._failed = True
            return False
        
        # grab the database we're connecting to
        db = connection[dbname]
        if (user or pword) and not db.authenticate(user, pword):
            msg = 'Authentication failed - %s, %s.' % (user, pword)
            logger.error(errors.DatabaseError(msg))
            self._failed = True
            return False
        
        self._connection = connection
        self._mongodb    = db
        
        return True
    
    def queryCommand(self, schema, query):
        """
        Converts the inputed query object to a SQL query command.
        
        :param      schema  <TableSchema> || None
        :param      query   <Query>
        :param      data    <dict>
        
        :return     <dict> commands
        """
        if query.isNull():
            logger.debug( 'Mongo.queryCommand: NULL QUERY.' )
            return {}
            
        # load query compoundss
        if orb.QueryCompound.typecheck(query):
            # extract the rest of the query information
            output      = {}
            or_results  = []
            
            for q in query.queries():
                result = self.queryCommand(schema, q)
                
                # merge nested results
                for key in result.keys():
                    if ( key in output and \
                         type(output[key]) == dict and \
                         type(result[key]) == dict ):
                        output[key].update(result[key])
                        result.pop(key)
                
                if query.operatorType() == orb.QueryCompound.Op.Or:
                    or_results.append(result)
                else:
                    output.update(result)
            
            if or_results:
                output['$or'] = or_results
            
            return output
            
        # load Query objects
        # initialize the field query objects
        if query.table():
            schema = query.table().schema()
        
        # make sure we have a schema to work with
        elif not schema:
            raise errors.QueryInvalid(query)
            
        value       = query.value()
        tableName   = schema.tableName()
        
        # mongo doesn't support date entries, must convert to datetime
        if type(value) == datetime.date:
            value = datetime.datetime(value.year, 
                                      value.month, 
                                      value.day, 
                                      0, 
                                      0, 
                                      0)
        
        op      = query.operatorType()
        colname = query.columnName()
        col     = schema.column(colname)
        if not col:
            warn = errors.ColumNotFoundError(colname, schema.name())
            logger.warning(warn)
            return {}
        
        # extract the primary key information
        if orb.Table.recordcheck(value):
            value = self.recordCommand(col, value)
        
        elif col.fieldName() == '_id':
            if isinstance(value, basestring):
                value = bson.objectid.ObjectId(value)
            
            elif type(value) in (list, tuple, set):
                values = []
                for val in value:
                    if ( isinstance(val, basestring) ):
                        val = bson.objectid.ObjectId(val)
                    values.append(val)
                value = values
        
        # extract the primary key information for a list of items
        elif type(value) in (list, tuple, set):
            value = [self.recordCommand(col, entry) for entry in value]
        
        # extract the primary key information from a record set
        elif isinstance(value, orb.RecordSet):
            value = value.values(orb.system.settings().primaryField())
        
        # make sure we're working with ObjectId's (kind of a pain)
        elif col.columnType() == orb.ColumnType.ForeignKey and \
             isinstance(value, basestring):
            value = bson.objectid.ObjectId(value)
        
        field    = col.fieldName()
        
        if op == Q.Op.IsNot:
            output = {field: {'$ne': value}}
            
        elif op == Q.Op.GreaterThan:
            output = {field: {'$gt': value}}
            
        elif op == Q.Op.LessThan:
            output = {field: {'$lt': value }}
        
        elif op == Q.Op.Between:
            output = {field: {'$lt': value[0], '$gt': value[1]}}
        
        elif op == Q.Op.GreaterThanOrEqual:
            output = {field: {'$gte': value}}
        
        elif op == Q.Op.LessThanOrEqual:
            output = {field: {'$lte': value }}
        
        elif op == Q.Op.IsIn:
            output = {field: {'$in': value }}
        
        elif op == Q.Op.IsNotIn:
            output = {field: {'$nin': value }}
        
        elif op in (Q.Op.Contains, Q.Op.Matches):
            output = {field: {'$regex': value, '$options': 'i'}}
        
        elif op == Q.Op.DoesNotMatch:
            output = {field: {'$not': re.compile(value)}}
        
        elif op == Q.Op.Startswith:
            output = {field: {'$regex': '^%s' % value, '$options': 'i' }}
        
        elif op == Q.Op.Endswith:
            output = {field: {'$regex': '%s$' % value, '$options': 'i'}}
        
        else:
            output = {field: value}
            
        return output
    
    def recordCommand(self, column, value):
        """
        Converts the inputed value from a record instance to a mongo id pointer,
        provided the value is a table type.  If not, the inputed value is 
        returned unchanged.
        
        :param      column | <orb.Column>
                    value  | <variant>
        
        :return     <variant>
        """
        # handle conversions of non record values to mongo object ids when
        # necessary
        if not orb.Table.recordcheck(value):
            if column.columnType() == orb.ColumnType.ForeignKey:
                if isinstance(value, basestring):
                    value = bson.objectid.ObjectId(value)
                elif type(value) in (list, tuple, set):
                    value = [self.recordCommand(column, val) for val in value]
                
            return value
        
        pkey = value.primaryKey()
        if not pkey:
            raise errors.PrimaryKeyNotDefined(value)
        
        if type(pkey) in (list, tuple, set):
            if len(pkey) == 1:
                pkey = pkey[0]
            else:
                pkey = tuple(pkey)
        
        if isinstance(value.database().backend(), Mongo):
            return bson.objectid.ObjectId(pkey)
        return pkey
    
    def removeRecords(self, remove, options):
        """
        Removes the inputed record from the database.
        
        :param      schema      | <orb.TableSchema>
                    record      | [<variant> primaryKey, ..]
                    options     | <orb.DatabaseOptions>
        
        :return     <int> number of rows removed
        """
        raise errors.DatabaseError('Invalid operation')
        
        db = self.mongodb()
        if not db:
            return 0
    
        tableName = schema.tableName()
        columns   = schema.primaryColumns()
        
        pkeys  = map(bson.objectid.ObjectId, records)
        spec   = {'_id': { '$in': pkeys }}
        count  = len(pkeys)
        
        if options.dryRun:
            logger.info('dryrun: removing from db: %s' % spec)
        else:
            db[schema.tableName()].remove(spec)
        
        return count
    
    def select(self, table_or_join, lookup, options):
        """
        Selects the records from the database for the inputed table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.DatabaseOptions>
        
        :return     [<variant> result, ..]
        """
        raw = self._select(table_or_join, lookup, options)
        
        output = []
        for schema, results in raw:
            for result in results:
                self.cleanValues(schema, result)
                
                # force the primary column to use the object id
                db_result = {}
                for field, value in result.items():
                    col = schema.column(field)
                    if ( col ):
                        db_result[col.name()] = value
                    else:
                        db_result[field] = value
                
                output.append(db_result)
        
        return output
    
    def tableExists(self, schema, options):
        """
        Checks to see if the inputed table class exists in the
        database or not.
        
        :param      schema  | <orb.TableSchema>
                    options | <orb.DatabaseOptions>
        
        :return     <bool> exists
        """
        db = self.mongodb()
        if not db:
            return False
        
        try:
            result = db.validate_collection(schema.tableName())
            return True
            
        except pymongo.errors.OperationFailure:
            return False
    
    def update(self, record, options):
        """
        Updates the modified data in the database for the 
        inputed record.  If the dryRun flag is specified then
        the command will be logged but not executed.
        
        :param      record  | <orb.Table>
                    options | <orb.DatabaseOptions>
        
        :return     <dict> changes
        """
        changes = record.changeset()
        
        # make sure we have some changes to commit
        if not len(changes):
            return {}
        
        # grab the primary key information
        pkey = record.primaryKey()
        if not pkey:
            raise errors.PrimaryKeyNotDefined(record)
        
        db = self.mongodb()
        if not db:
            return {'db_error': 'Could not connect to database.'}
        
        pkey = record.primaryKey()
        schema = record.schema()
        tableName = schema.tableName()
        updates = {'$set': {}}
        
        for colname, changevals in changes.items():
            column = schema.column(colname)
            newValue = changevals[1]
            
            if not column:
                raise errors.ColumnNotFound(colname)
            
            elif column.required() and newValue == None:
                raise errors.ColumnRequired(colname)
            
            elif orb.Table.recordcheck(newValue):
                pkeyValue = newValue.primaryKey()
                if not pkeyValue:
                    raise errors.PrimaryKeyNotDefined(newValue)
                
                if isinstance(newValue.database().backend(), Mongo):
                    newValue = bson.objectid.ObjectId(pkeyValue)
                else:
                    newValue = pkeyValue
            
            updates['$set'][column.fieldName()] = newValue
        
        if not updates['$set']:
            return {}
        
        obj_id = bson.objectid.ObjectId(pkey)
        
        if options.dryRun:
            logger.info('Updating %s, %s, %s' % (obj_id, type(obj_id), updates))
        else:
            db[tableName].update({'_id': obj_id}, updates)
        
        return changes
    
    def updateTable(self, schame, options):
        """
        Determines the difference between the inputed schema
        and the table in the database, creating new columns
        for the columns that exist in the schema and do not
        exist in the database.  If the dryRun flag is specified,
        then the SQL won't actually be executed, just logged.

        :note       This method will NOT remove any columns, if a column
                    is removed from the schema, it will simply no longer 
                    be considered part of the table when working with it.
                    If the column was required by the db, then it will need to 
                    be manually removed by a database manager.  We do not
                    wish to allow removing of columns to be a simple API
                    call that can accidentally be run without someone knowing
                    what they are doing and why.
        
        :param      schema    | <orb.TableSchema>
                    options   | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        # mongo updates tables on the fly - do not need to explicitly run this
        # command
        return True
    
# register the postgres backend
if pymongo:
    orb.Connection.registerAddon('Mongo', Mongo)


