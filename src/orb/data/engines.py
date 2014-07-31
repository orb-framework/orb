#!/usr/bin/python

"""
Defines the various engines that will be used by the backend plugins to
generate content.
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

import datetime
import projex.rest
import projex.text
import pytz
import random
import yaml

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr

errors = LazyModule('orb.errors')

MAX_INT = 2**64-1


class CommandEngine(object):
    """ Defines a class for generating query statments for a table """
    
    def __init__(self, backend, wrapper='`'):
        self._backend = backend
        self._commands = {}
        self._stringWrapper = wrapper
    
    def backend(self):
        """
        Returns the backend associated with this engine.
        
        :return     <orb.Connection>
        """
        return self._backend
    
    def clearNicknames(self, setup):
        setup['curr_nicknames'] = {}
    
    def command(self, key):
        """
        Returns the command for the given key.
        
        :param      key | <variant>
        
        :return     <str> | command
        """
        return self._commands.get(key, '').strip()
    
    def currentNickname(self, tableName, setup):
        try:
            return setup['curr_nicknames'][tableName]
        except KeyError:
            return tableName
    
    def genkey(self, key):
        """
        Generates a random key for the key in the data dictionary
        for the inputed key.
        
        :param      key | <str>
        """
        return '{0}-{1:x}-{2:x}'.format(key,
                                        random.randint(0, MAX_INT),
                                        random.randint(0, MAX_INT))
    
    def nextNickname(self, tableName, setup):
        """
        Defines the next nickname for this table system.
        
        :param      tableName | <str>
                    setup | <dict>
        """
        setup.setdefault('nicknames', {})
        setup.setdefault('curr_nicknames', {})
        
        index = setup['nicknames'].get(tableName, 1)
        nickname = '{0}_{1}'.format(tableName, index)
        setup['nicknames'][tableName] = index + 1
        setup['curr_nicknames'][tableName] = nickname
        return nickname
    
    def setCommand(self, key, command):
        """
        Sets the command for the given key to the inputed command.
        
        :param      key     | <variant>
                    command | <str>
        """
        self._commands[key] = command

    def setStringWrapper(self, wrapper):
        """
        Sets the string wrapper for this command engine.
        
        :param      wrapper | <str>
        """
        self._stringWrapper = wrapper

    def stringWrapper(self):
        """
        Returns the string wrapper for this command engine.
        
        :return     <str>
        """
        return self._stringWrapper
    
    def wrapString(self, *args, **kwds):
        """
        Wraps the protected text in a database specific way.  This will ensure
        the given text is treated as a particular name in the DB vs. a possible
        keyword that would cause a query error.
        
        :param      text | <str>
        
        :return     <str>
        """
        sep = kwds.get('separator', '.')
        ch = self.stringWrapper()
        return sep.join(map(lambda x: ch + x + ch, args))

#----------------------------------------------------------------------

class ColumnEngine(CommandEngine):
    def addCommand(self, column):
        """
        Returns the command used for adding this column to a table.
        
        :return     <str>
        """
        return ''
    
    def createCommand(self, column):
        """
        Returns the creation command for this column.
        
        :return     <str>
        """
        return ''
    
    def queryCommand(self,
                     column,
                     op,
                     value,
                     offset='',
                     caseSensitive=False,
                     setup=None,
                     language=None):
        """
        Converts the inputed column, operator and value to a query statement.
        
        :param      columns       | <orb.TableSchema>
                    op            | <orb.Column.Op>
                    value         | <variant>
                    caseSensitive | <bool>
        
        :return     <str> cmd, <dict> data
        """
        return '', {}
    
    def fromString(self, value_str):
        """
        Converts the inputed string to a value for this engine.
        
        :param      value_str | <str>
        
        :return     <variant>
        """
        try:
            return eval(value_str)
        except:
            return value_str
    
    def toString(self, value):
        """
        Converts the inputed value to a string representation.
        
        :param      value | <variant>
        
        :return     <str>
        """
        return nstr(value)
    
    def unwrap(self, column, value):
        """
        Unwraps the inputed value from the database to the proper Python value.
        
        :param      value | <variant>
        """
        coltype = ColumnType.base(column.columnType())
        
        if value is None:
            return None
        
        # unwrap a pickle value
        elif coltype == ColumnType.Pickle:
            try:
                return cPickle.loads(nstr(value))
            except StandardError:
                return None
        
        # expand yaml data
        elif coltype == ColumnType.Yaml:
            try:
                return yaml.loads(nstr(value))
            except StandardError:
                return None
        
        # generate a query from XML
        elif coltype == ColumnType.Query:
            if type(value) == dict:
                return orb.Query.fromDict(value)
            else:
                try:
                    return orb.Query.fromXmlString(nstr(value))
                except StandardError:
                    return None
        
        # return a data dictionary
        elif coltype == ColumnType.Dict:
            return projex.rest.dejsonify(value)
        
        # return a string value
        elif column.isString():
            return projex.text.decoded(value)
        
        return value
    
    def wrap(self, column, value):
        """
        Converts the inputed value to be able to be stored in the database.
        
        :param      value | <variant>
        """
        coltype = ColumnType.base(column.columnType())
        
        # always allow NULL types
        if value is None:
            return value
        
        # convert Query to xml
        elif coltype == ColumnType.Query:
            if type(value) == dict:
                value = orb.Query.fromDict(value)
            
            try:
                return value.toXmlString()
            except StandardError:
                return None
        
        # convert a pickle value
        elif coltype == ColumnType.Pickle:
            return cPickle.dumps(value)
        
        # convert to yaml
        elif coltype == ColumnType.Yaml:
            try:
                return yaml.dumps(value)
            except StandardError:
                return None
        
        # lookup records set values
        elif orb.RecordSet.typecheck(value):
            return value.primaryKeys()
        
        # lookup a record
        elif orb.Table.recordcheck(value):
            nvalue = value.primaryKey()
            if not nvalue:
                return None
            return nvalue
        
        # convert list/tuple information to clean values
        elif type(value) in (list, tuple, set):
            return tuple(map(lambda x: self.wrap(column, x), value))
        
        # convert timedelta information
        elif type(value) == datetime.timedelta:
            now = datetime.datetime.now()
            dtime = now + value
            return self.wrap(column, dtime)
        
        # convert timezone information
        elif type(value) == datetime.datetime:
            # convert timezone information to UTC data
            if value.tzinfo is not None:
                return value.astimezone(pytz.utc).replace(tzinfo=None)
            return value
        
        # convert dictionary data to database
        elif type(value) == dict:
            return projex.rest.jsonify(value)
        
        # convert string information
        elif column is not None and column.isString():
            return projex.text.decoded(value)
        
        return value

#----------------------------------------------------------------------

class SchemaEngine(CommandEngine):
    def alterCommand(self, schema, columns=None, ignore=None):
        """
        Generates the alter table command.
        
        :param      schema | <orb.TableSchema>
                    db     | <orb.Database> || None
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def columnsCommand(self, schema):
        """
        Returns the command for the inputed schema to lookup its
        columns from the database.
        
        :return     <str>, <dict>
        """
        return '', {}
    
    def countCommand(self, schemas, **options):
        """
        Returns the command that will be used to calculate the count that
        will be returned for the given query options.
        
        :param      schemas   | [<orb.TableSchema>, ..]
                    **options | database options
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def createCommand(self, schema):
        """
        Generates the table creation command.
        
        :param      schema | <orb.TableSchema>
                    db     | <orb.Database> || None
                    
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def deleteCommand(self, schema, records):
        """
        Generates the table deletion command.
        
        :param      schema  | <orb.TableSchema>
                    records | [<variant> pkey, ..]
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def disableInternalsCommand(self, schema):
        """
        Generates the disable internals command for this schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def enableInternalsCommand(self, schema):
        """
        Generates the enable internals command for this schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def existsCommand(self, schema):
        """
        Returns the command that will determine whether or not the schema
        exists in the database.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def insertCommand(self, schema, records, columns=None):
        """
        Generates the table insertion command.
        
        :param      schema  | <orb.TableSchema>
                    records | [<orb.Table>, ..]
                    columns | [<str>, ..] || None
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def queryCommand(self, schemas, query, setup=None, language=None):
        """
        Generates the query command for the given information.
        
        :param      schemas | <orb.TableSchema>
                    query   | <orb.Query>
                    setup   | <dict> || None
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def queryCompoundCommand(self, schemas, query, setup=None):
        """
        Generates the query compound command for the given information.
        
        :param      schemas | <orb.TableSchema>
                    query   | <orb.QueryCompound>
                    setup   | <dict> || None
        
        :return     <str> command, <dict> data
        """
        where_commands = []
        having_commands = []
        data = {}
        
        for q in query.queries():
            where_command, having_command, qdata = self.whereCommand(schemas,
                                                                     q,
                                                                     setup)
            
            if where_command:
                where_commands.append(where_command)
            if having_command:
                having_commands.append(having_command)
            
            if where_command or having_command:
                data.update(qdata)
        
        optype  = query.operatorType()
        strtype = orb.QueryCompound.Op[optype].upper()
        joiner  = ' {0} '.format(strtype)
        
        if where_commands:
            where_command = '({0})'.format(joiner.join(where_commands))
        else:
            where_command = ''
        
        if having_commands:
            having_command = '({0})'.format(joiner.join(having_commands))
        else:
            having_command = ''
        
        if not (having_command or where_command):
            data = {}
        
        return where_command, having_command, data
    
    def queryOffset(self, schemas, query, setup=None):
        """
        Generates the offset query information
        """
        typ   = query.offsetType()
        value = query.offsetValue()
        
        # generate the offset value query
        if Query.typecheck(value):
            value_str, data = self.queryValue(schemas, value, setup)
        else:
            column = query.column(schemas[0])
            engine = column.engine(self.backend().database())
            if engine:
                value = engine.wrap(column, value)
            
            key = self.genkey('offset')
            data = {key: value}
            value_str = '%({0})s'.format(key)
        
        symbol = Query.OffsetSymbol.get(typ)
        if symbol:
            return (symbol + value_str, data)
        else:
            return ('', {})
    
    def queryValue(self, schemas, query, setup=None):
        """
        Converts a query to the database lookup value.
        
        :param      schemas | [<orb.TableSchema>, ..]
                    query   | <orb.Query>
                    setup   | <dict> || None
        
        :return     (<str> command, <dict> data)
        """
        column = query.column(schemas[0])
        if not column:
            table = schemas[0].name()
            raise errors.ColumnNotFoundError(query.columnName(), table)
        
        if self.backend().isObjectOriented():
            schema = column.firstMemberSchema(schemas)
        else:
            schema = column.schema()
        
        cmd = self.wrapString(schema.tableName(), column.fieldName())
        if query.isOffset():
            offset, data = self.queryOffset(schemas, query, setup)
            cmd += offset
        else:
            data = {}
        
        return cmd, data
        
    def removeCommand(self, schema, records):
        """
        Generates the command for removing the inputed records from the
        database.
        
        :param      schema  | <orb.TableSchema>
                    records | [<orb.Table>, ..]
        
        :return     <str> command, <dict> data
        """
        return '', {}
        
    def selectCommand(self, schemas, **options):
        """
        Generates the command that will be used when selecting records from
        the database.
        
        :param      schemas | [<orb.TableSchema>, ..]
                    **options | query options
        
        :return     <str> command, <dict> data
        """
        return '', {}
    
    def updateCommand(self, schema, records, columns=None):
        """
        Generates the command that will be used when updating records within
        the database.
        
        :param      schema | <orb.TableSchema>
                    records | [<orb.Table>, ..]
                    columns | [<str>, ..] || None
        
        :return     <str> command, <dict> data
        """
        return '', {}

    def whereCommand(self, schemas, where, setup=None):
        """
        Generates the query command for the given information.
        
        :param      schemas | <orb.TableSchema>
                    where   | <orb.Query> || <orb.QueryCompound>
                    setup   | <dict> || None
        
        :return     <str> where_command, <str> having_command, <dict> data
        """
        # generate sub-query options
        if orb.QueryCompound.typecheck(where):
            return self.queryCompoundCommand(schemas, where, setup)
        elif where:
            column = where.column(schemas[0])
            cmd, data = self.queryCommand(schemas, where, setup)
            
            if column is not None and column.isAggregate():
                return '', cmd, data
            else:
                return cmd, '', data
        else:
            return '', '', {}
    