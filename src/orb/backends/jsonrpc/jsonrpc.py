#!/usr/bin/python

""" Defines the backend connection class for JSONRPC remote API's. """

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
__depends__        = ['']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__


import logging
import orb
import projex.rest
import threading
import traceback

from orb import errors
from projex.text import nativestring as nstr

try:
    import requests
except ImportError:
    requests = None

log = logging.getLogger(__name__)

# assign record encode/decoders
def record_encoder(py_obj):
    # encode a record
    if orb.Table.recordcheck(py_obj):
        return True, py_obj.json()
    # encode a recordset
    elif orb.RecordSet.typecheck(py_obj):
        return True, [record.json() for record in py_obj]
    # encode a query
    elif orb.Query.typecheck(py_obj):
        return True, py_obj.toDict()
    return False, None

projex.rest.register(record_encoder)


#------------------------------------------------------------------------------

class JSONRPC(orb.Connection):
    def __init__(self, database):
        super(JSONRPC, self).__init__(database)
        
        self.setThreadEnabled(True)
    
    def close(self):
        """
        Closes the connection to the datbaase for this connection.
        
        :return     <bool> closed
        """
        return True
    
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return     <bool> success
        """
        return True
    
    def count( self, table_or_join, lookup, options ):
        """
        Returns the number of records that exist for this connection for
        a given lookup and options.
        
        :sa         distinct, select
        
        :param      table_or_join | <orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     <int>
        """
        # ensure we're working with a valid table
        if not orb.Table.typecheck(table_or_join):
            log.debug('JSONRPC backend only supports table lookups.')
            return {}
        
        # create the query data
        py_data = {'table': table_or_join.schema().name(),
                   'lookup': lookup.toDict(),
                   'options': options.toDict()}
        
        response = self.execute('count', py_data)
        return int(response.get('count', 0))
    
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
        # ensure we're working with a valid table
        if not orb.Table.typecheck(table_or_join):
            log.debug('JSONRPC backend only supports table lookups.')
            return {}
        
        # create the query data
        py_data = {'table': table_or_join.schema().name(),
                   'lookup': lookup.toDict(),
                   'options': options.toDict()}
        
        response = self.execute('distinct', py_data)
        if not 'records' in response:
            raise errors.InvalidResponse('distinct', response)
        
        records = response['records']
        if type(records) == list:
            return {lookup.columns[0]: records}
        else:
            return records
    
    def execute(self, command, data=None, flags=None):
        """
        Executes the inputed command into the current 
        connection cursor.
        
        :param      command  | <str>
                    data     | <dict> || None
                    flags    | <orb.DatabaseFlags>
        
        :return     <variant> returns a native set of information
        """
        log.debug('thread: %s', threading.current_thread().ident)
        
        # ensure we have a database
        db = self.database()
        if not db:
            raise errors.DatabaseNotFound()
        
        # generate the json rpc API url
        url = db.host().rstrip('/') + '/' + command
        
        # convert the data to json format
        json_data = projex.rest.jsonify(data)
        headers = {'content-type': 'application/json'}
        log.debug('#------------------------------------------------')
        log.debug('url: %s', url)
        log.debug('data: %s', json_data)
        log.debug('headers: %s', headers)
        
        if db.credentials():
            log.debug('auth: REQUIRED')
        
        log.debug('#------------------------------------------------')
        try:
            response = requests.post(url,
                                     data=json_data,
                                     headers=headers,
                                     auth=db.credentials())
        except StandardError as err:
            log.debug('Error processing request.\n%s', err)
            response = None
        
        # process the response information
        if response:
            if response.status_code == 200:
                try:
                    results = projex.rest.unjsonify(response.content)
                    if results.get('status_code', 200) != 200:
                        log.error(results.get('msg'))
                        return {}
                    else:
                        return results
                except ValueError:
                    log.error('Failed to unjsonify: %s', response.content)
                    return {}
            else:
                log.error('{0} Error: %s'.format(response.status_code), response.content)
                return {}
        else:
            return {}
    
    def insert(self, records, lookup, options):
        """
        Inserts the database record into the database with the
        given values.
        
        :param      record      | <orb.Table>
                    options     | <orb.DatabaseOptions>
        
        :return     <bool>
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif orb.Table.recordcheck(records):
            records = [records]
        
        submit = []
        submit_records = []
        changeset = []
        for record in records:
            changes = record.changeset(columns=lookup.columns)
            if not changes:
                continue
            
            changeset.append(changes)
            table = record.schema().name()
            
            values = dict([(x, y[1]) for x, y in changes.items()])
            values = self.processValue(values)
            
            submit_records.append(record)
            submit.append({'table': table,
                           'id': record.primaryKey(),
                           'values': values})
        
        # create the query data
        if not submit:
            return {}
        
        py_data = {'records': submit}
        response = self.execute('insert', py_data)
        
        if 'error' in response:
            raise errors.InvalidResponse('insert', response['error'])
        
        # update the records
        ids = response.get('ids')
        if not (ids and len(ids) == len(submit_records)):
            raise errors.InvalidResponse('insert', 
                                         'No IDs were returned from insert.')
        else:
            for i, record in enumerate(submit_records):
                record._updateFromDatabase({'PRIMARY_KEY': ids[i]})
                record._markAsLoaded(self.database(), columns=lookup.columns)
        
        if len(changeset) == 1:
            return changeset[0]
        return changeset
    
    def isConnected(self):
        """
        Returns whether or not this conection is currently
        active.
        
        :return     <bool> connected
        """
        return True
    
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
        py_data = {'options': options.toDict()}
        
        queries = []
        for table, query in remove.items():
            queries.append({'table': table.schema().name(),
                            'query': [q.toDict() for q in query]})
        
        py_data['queries'] = queries
        response = self.execute('remove', py_data)
        return response.get('removed', 0)
    
    def select(self, table_or_join, lookup, options):
        """
        Selects the records from the database for the inputed table or join
        instance based on the given lookup and options.
                    
        :param      table_or_join   | <subclass of orb.Table>
                    lookup          | <orb.LookupOptions>
                    options         | <orb.DatabaseOptions>
        
        :return     [<variant> result, ..]
        """
        if not lookup.order and orb.Table.typecheck(table_or_join):
            lookup.order = table_or_join.schema().defaultOrder()
        
        # ensure we're working with a valid table
        if not orb.Table.typecheck(table_or_join):
            log.debug('JSONRPC backend only supports table lookups.')
            return {}
        
        # create the query data
        py_data = {'table': table_or_join.schema().name(),
                   'lookup': lookup.toDict(),
                   'options': options.toDict()}
        
        response = self.execute('select', py_data)
        if 'error' in response:
            raise errors.InvalidResponse('select', response['error'])
        
        elif not 'records' in response:
            raise errors.InvalidResponse('select', response)
        
        records = response.get('records', [])
        return records
    
    def open(self):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :return     <bool> success
        """
        return True
    
    def rollback(self):
        """
        Rollsback the latest code run on the database.
        """
        return False
    
    def update(self, records, lookup, options):
        """
        Updates the database record into the database with the
        given values.
        
        :param      record  | <orb.Table>
                    options | <orb.DatabaseOptions>
        
        :return     <bool>
        """
        # convert the recordset to a list
        if orb.RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif orb.Table.recordcheck(records):
            records = [records]
        
        submit_records = []
        submit = []
        changeset = []
        for record in records:
            changes = record.changeset(columns=lookup.columns)
            if not changes:
                continue
            
            changeset.append(changes)
            
            table = record.schema().name()
            
            values = dict([(x, y[1]) for x, y in changes.items()])
            values = self.processValue(values)
            
            submit_records.append(record)
            submit.append({'table': table,
                           'id': record.primaryKey(),
                           'values': values})
        
        # create the query data
        if not submit:
            return {}
        
        py_data = {'records': submit}
        response = self.execute('update', py_data)
        
        if 'error' in response:
            print traceback.print_exc()
            raise errors.InvalidResponse('update', response['error'])
        else:
            for i, record in enumerate(submit_records):
                record._markAsLoaded(self.database(), columns=lookup.columns)
        
        if len(changeset) == 1:
            return changeset[0]
        return changeset
    
    @staticmethod
    def processValue(value):
        if orb.Table.recordcheck(value):
            return value.primaryKey()
        elif type(value) == dict:
            out = {}
            for key, val in value.items():
                out[key] = JSONRPC.processValue(val)
            return out
        elif type(value) in (list, tuple):
            typ = type(value)
            out = []
            for val in value:
                out.append(JSONRPC.processValue(val))
            return typ(out)
        else:
            return value
    
    @staticmethod
    def serve(method,
              json_data,
              database=None,
              username=None,
              password=None):
        """
        Processes the method from the network.  This method should be used on
        the server side to process client requests that are being generated
        using this JSONRPC backend.  You will need to call it from your web
        server passing along the requested data, and will generate a response
        in a JSON formated dictionary to be passed back.
        
        :param      method | <str>
                    json_data | <str>
        
        :return     <str> | json_reponse
        """
        # processing the inputed json information
        try:
            py_data  = projex.rest.unjsonify(json_data)
        except StandardError:
            msg = 'Failed to load the JSONRPC options.'
            return projex.rest.jsonify({'status_code': 500,
                                        'status': 'error',
                                         'msg': msg})
        
        database = py_data.get('database', database)
        table    = None
        record   = None
        lookup   = None
        options  = None
        values   = None
        ids      = None
        records  = None
        response = {}
        
        # extract the table from the call
        table = None
        if 'table' in py_data:
            table = orb.system.model(py_data['table'], database=database)
            if not table:
                return {'error': 'No %s table found.' % py_data['table']}
        
        # extract lookup information from the call
        if 'lookup' in py_data:
            lookup = orb.LookupOptions.fromDict(py_data['lookup'])
        
        # extract option information from the call
        if 'options' in py_data:
            options = orb.DatabaseOptions.fromDict(py_data['options'])
        
        # extract id information
        if 'ids' in py_data:
            ids = py_data['ids']
        
        # extract the record id information
        if 'record' in py_data:
            if not table:
                return {'error': 'No %s table found for record.'}
            
            record = table(py_data['record'])
        
        # extract records information
        elif 'records' in py_data:
            records = []
            for submit in py_data['records']:
                table = orb.system.model(submit['table'], database=database)
                if not table:
                    continue
                
                if 'id' in submit:
                    record = table(submit['id'])
                    values = submit['values']
                    record._Table__record_values.update(values)
                    records.append(record)
                
                elif 'ids' in submit:
                    q = orb.Query(table).in_(submit['ids'])
                    records += table.select(where=q).records()
        
        # extract value information from the call
        if 'values' in py_data:
            values = py_data.get('values')
            if record:
                record._Table__record_values.update(values)
        
        #----------------------------------------------------------------------
        # process methods
        #----------------------------------------------------------------------
        
        # get a count from the system
        if method == 'count':
            rset = table.select(lookup)
            rset.setDatabaseOptions(options)
            response = {'count': len(rset)}
        
        # perform a distinct select
        elif method == 'distinct':
            rset = table.select(lookup)
            rset.setDatabaseOptions(options)
            records = rset.distinct(lookup.columns, inflated=False)
            response = {'records': records}
        
        # insert a record into the database
        elif method == 'insert':
            rset = orb.RecordSet(records)
            try:
                rset.commit()
                response['ids'] = rset.primaryKeys()
            except StandardError, err:
                response['error'] = nstr(err)
        
        # remove records from the database
        elif method == 'remove':
            # 2.0 version
            if 'queries' in py_data:
                count = 0
                for rem_data in py_data['queries']:
                    tableName = rem_data['table']
                    query_list = rem_data['query']
                    table = orb.system.model(tableName)
                    if not table:
                        continue
                    
                    for query_dict in query_list:
                        query = orb.Query.fromDict(query_dict)
                        records = table.select(where=query)
                        count += records.remove(options=options)
                
                response = {'removed': count}
            
            # 1.5 version
            elif records is not None:
                records = orb.RecordSet(records)
                response = {'removed': records.remove(options=options)}
            
            # 1.0 version
            elif ids is None:
                response = {'error': 'No ids were supplied for removal'}
            else:
                count = table.select(where=orb.Query(table).in_(ids)).remove()
                response = {'removed': count}
        
        # perform a select
        elif method == 'select':
            rset = table.select(lookup)
            rset.setDatabaseOptions(options)
            response = {'records': rset.records(inflated=False, ignoreColumns=True)}
        
        # perform an update
        elif method == 'update':
            rset = orb.RecordSet(records)
            try:
                rset.commit()
            except StandardError, err:
                response['error'] = nstr(err)
        
        # lookup not found
        else:
            response = {'error': '%s is an invalid json call'}
        
        return projex.rest.jsonify(response)
    
if requests:
    orb.Connection.registerAddon('JSONRPC', JSONRPC)