#!/usr/bin/python

""" Defines the backend connection class for basic SQL based databases. """

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
import threading
import time

import projex.iters
import projex.errors
import projex.text
import projex.rest

from projex.text import nativestring
from projex.decorators import abstractmethod

import orb
from orb                 import errors, Orb, settings
from orb.common          import ColumnType, SelectionMode
from orb.connection      import Connection,\
                                DatabaseOptions,\
                                SchemaEngine,\
                                ColumnEngine
from orb.transaction     import Transaction
from orb.table           import Table
from orb.column          import Column
from orb.join            import Join
from orb.recordset       import RecordSet
from orb.query           import Query as Q, QueryCompound
from orb.common          import CallbackType

logger = logging.getLogger(__name__)

#----------------------------------------------------------------------
#                       DEFAULT SQL COMMANDS
#----------------------------------------------------------------------

DEFAULT_COMMON_CMDS = {
}

DEFAULT_SCHEMA_CMDS = {
'alter':
    u"""
    ALTER TABLE {table} {columns}
    """,
'aggregate':
    u"""
    LEFT OUTER JOIN {table} as {aggregate} ON {where}
    """,
'aggr_count':
    u"""
    COUNT(DISTINCT {table}.{colname})
    """,
'constraint_pkey':
    u"""
    CONSTRAINT {table} PRIMARY KEY ({columns})
    """,
'count':
    u"""
    SELECT COUNT(*) AS count FROM ({select_command}) AS records;
    """,
'create':
    u"""
    CREATE TABLE {table} ({columns}{constraints});
    """,
'create_inherited':
    u"""
    CREATE TABLE {table} (
        {inherit_id} INTEGER NOT NULL {columns}{constraints}
    );
    """,
'delete':
    u"""
    DELETE FROM {table}{join}{where};
    """,
'delete_join':
    u"""
    USING {tables}
    """,
'disable_internals':
    u"""
    SET unique_checks=0;
    SET foreign_key_checks=0;
    """,
'disable_table_internals':
    u"""
    ALTER TABLE {table} DISABLE KEYS;
    """,
'enable_table_internals':
    u"""
    ALTER TABLE {table} ENABLE KEYS;
    """,
'enable_internals':
    u"""
    SET unique_checks=1;
    SET foreign_key_checks=1;
    """,
'inherit_id':
    u"""
    __inherits__
    """,
'insert':
    u"""
    INSERT INTO {table}
    ({columns})
    VALUES
    {values};
    """,
'inner_join':
    u"""
    LEFT JOIN {table} AS {nickname} ON {where}
    """,
'outer_join':
    u"""
    LEFT OUTER JOIN {table} AS {nickname} ON {where}
    """,
'next_id':
    u"""
    SELECT _id+1 AS next_id FROM {table} ORDER BY _id DESC LIMIT 1;
    """,
'primary_id':
    u"""
    _id
    """,
'select':
    u"""
    SELECT {select_options} {columns}
    FROM {tables}{joins}{where}{group_by}{having}{lookup_options};
    """,
'truncate':
    u"""
    TRUNCATE {table};
    """,
'update':
    u"""
    UPDATE {table}
    SET {values}
    WHERE {where};
    """
}

DEFAULT_COLUMN_CMDS = {
'add':
    u"""
    ADD COLUMN {column} {type}{maxlen}{flags}
    """,
'add_ref':
    u"""
    ADD COLUMN {column} {type}{maxlen}{flags}
    """,
'create':
    u"""
    {column} {type}{maxlen}{flags}
    """,
'create_ref':
    u"""
    {column} {type}{maxlen}{flags}
    """,
'aggr_count':
    u"""
    COUNT(DISTINCT {table}.{colname})
    """,

# flag commands
Column.Flags.Required:      u'NOT NULL',
Column.Flags.Unique:        u'UNIQUE',
Column.Flags.AutoIncrement: u'AUTO_INCREMENT',

# operator options
'Is':                   u'=',
'IsNot':                u'!=',
'LessThan':             u'<',
'Before':               u'<',
'LessThanOrEqual':      u'<=',
'GreaterThanOrEqual':   u'>=',
'GreaterThan':          u'>',
'After':                u'>',
'Matches':              u'~',
'DoesNotMatch':         u'!~',
'ContainsSensitive':    u'LIKE',
'ContainsInsensitive':  u'ILIKE',
'IsIn':                 u'IN',
'IsNotIn':              u'NOT IN',

# function options
'Lower': u'lower({0})',
'Upper': u'upper({0})',
'Abs': u'abs({0})',
'AsString': u"{0}::varchar"
}
DEFAULT_COLUMN_CMDS.update(DEFAULT_COMMON_CMDS)

DEFAULT_TYPE_MAP = {
    ColumnType.Bool:        u'BOOL',
        
    ColumnType.Decimal:     u'DECIMAL UNSIGNED',
    ColumnType.Double:      u'DOUBLE UNSIGNED',
    ColumnType.Integer:     u'INT UNSIGNED',
    ColumnType.Enum:        u'INT UNSIGNED',
    ColumnType.BigInt:      u'BIGINT UNSIGNED',

    ColumnType.ForeignKey:  u'BIGINT UNSIGNED',

    ColumnType.Datetime:             u'DATETIME',
    ColumnType.Date:                 u'DATE',
    ColumnType.Time:                 u'TIME',
    ColumnType.DatetimeWithTimezone: u'TIMESTAMP',

    ColumnType.String:      u'VARCHAR',
    ColumnType.Color:       u'VARCHAR',
    ColumnType.Email:       u'VARCHAR',
    ColumnType.Password:    u'VARCHAR',
    ColumnType.Url:         u'VARCHAR',
    ColumnType.Filepath:    u'VARCHAR',
    ColumnType.Directory:   u'VARCHAR',
    ColumnType.Text:        u'TEXT',
    ColumnType.Xml:         u'TEXT',
    ColumnType.Html:        u'TEXT',

    ColumnType.ByteArray:   u'VARBINARY',
    ColumnType.Yaml:        u'TEXT',
    ColumnType.Query:       u'TEXT',
    ColumnType.Pickle:      u'BLOB',
    ColumnType.Dict:        u'BLOB',
    ColumnType.Image:       u'BLOB'
}

DEFAULT_LENGTHS = {
    ColumnType.String:    256,
    ColumnType.Color:     25,
    ColumnType.Email:     256,
    ColumnType.Password:  256,
    ColumnType.Url:       500,
    ColumnType.Filepath:  500,
    ColumnType.Directory: 500
}


#----------------------------------------------------------------------
#                           CLASS DEFINITIONS
#----------------------------------------------------------------------

class SqlColumnEngine(ColumnEngine):
    def __init__(self, backend, sqltype, wrapper=u'`', cmds=None):
        super(SqlColumnEngine, self).__init__(backend)
        
        # define custom properties
        self._sqltype = sqltype
        
        # define flag sql information
        self.setStringWrapper(wrapper)
        if cmds is not None:
            for key, cmd in cmds.items():
                self.setCommand(key, cmd)
    
    def addCommand(self, column, language=None):
        """
        Generates the SQL command required for adding this column to the
        table.
        
        :param      column | <orb.Column>
        
        :return     <str>
        """
        cmd = u''
        if column.testFlag(Column.Flags.Primary):
            cmd = self.command(u'add_primary')
        elif column.reference():
            cmd = self.command(u'add_ref')
        if not cmd:
            cmd = self.command(u'add')
        
        return cmd.format(**self.options(column, language=language))
    
    def createCommand(self, column, language=None):
        """
        Creates the SQL commands for generating the given column.
        
        :param      column | <orb.Column>
        
        :return     <str>
        """
        cmd = u''
        if column.testFlag(Column.Flags.Primary):
            cmd = self.command(u'create_primary')
        elif column.reference():
            cmd = self.command(u'create_ref')
        if not cmd:
            cmd = self.command(u'create')
        
        return cmd.format(**self.options(column, language=language))
    
    def options(self, column, language=None):
        """
        Generates the options required for this column.
        
        :return     <dict>
        """
        opts = {}
        opts['column'] = self.wrapString(column.fieldName(language))
        opts['type'] = self._sqltype
        
        if column.reference():
            refmodel = column.referenceModel()
            if refmodel:
                refname = self.wrapString(refmodel.schema().tableName())
                opts['reference'] = refname
            else:
                raise errors.TableNotFoundError(column.reference())
        else:
            opts['reference'] = u''
        
        # assign the max length value
        maxlen = column.maxlength()
        if not maxlen:
            maxlen = DEFAULT_LENGTHS.get(column.columnType())
        if maxlen:
            opts['maxlen'] = u'({})'.format(maxlen)
        else:
            opts['maxlen'] = u''
        
        # format the flag information
        flag_opts = []
        for flag in column.iterFlags():
            cmd = self.command(flag)
            if cmd:
                flag_opts.append(cmd.format(**opts))
        
        if flag_opts:
            opts['flags'] = u' ' + u' '.join(flag_opts)
        else:
            opts['flags'] = u''
        
        return opts
    
    def queryCommand(self,
                     schema,
                     column,
                     op,
                     value,
                     offset=u'',
                     caseSensitive=False,
                     functions=None,
                     setup=None,
                     language=None):
        """
        Converts the inputed column, operator and value to a query statement.
        
        :param      columns       | <orb.TableSchema>
                    op            | <orb.Column.Op>
                    value         | <variant>
                    offset        | <str>
                    caseSensitive | <bool>
                    functions     | [<str>, ..] || None
                    setup         | <dict> || None
        
        :return     <str> cmd, <dict> data
        """
        if setup is None:
            setup = {}
        if functions is None:
            functions = []
        
        data = {}
        
        if column.isJoined():
            if not column in setup['column_aliases']:
                return u'', data
            
            schema_name = setup['column_aliases'][column]
            column = column.joiner()[0]
            schema = column.schema()
            sqlfield = self.wrapString(schema_name, column.fieldName()) + offset
            
        elif column.isAggregate():
            if not column in setup['column_aliases']:
                return u'', data
            
            aggr = column.aggregate()
            
            cmd = self.command(u'aggr_{0}'.format(aggr.type()))
            if not cmd:
                raise errors.UnsupportedWhereAggregate(aggr.type())
            
            col_schema = aggr.table().schema()
            
            opts = {}
            opts['table'] = self.wrapString(setup['column_aliases'][column])
            if col_schema.inherits() and not self.backend().isObjectOriented():
                opts['colname'] = self.wrapString('__inherits__')
            else:
                pcol = col_schema.primaryColumns()[0]
                opts['colname'] = self.wrapString(pcol.fieldName())
            
            sqlfield = cmd.format(**opts)
        
        else:
            schema_name = column.schema().tableName()
            schema_name = self.currentNickname(schema_name, setup)
            sqlfield = self.wrapString(schema_name,
                                       column.fieldName(language)) + offset
        
        # join functions together for the sqlfield
        for function in functions:
            func_cmd = self.command(function)
            if func_cmd:
                sqlfield = func_cmd.format(sqlfield)
        
        value    = self.wrap(column, value)
        field    = column.fieldName(language)
        vkey     = self.genkey(u'query_' + field)
        
        # generate the between query
        if op == Q.Op.Between:
            cmd = u'(%({0})s <= {1} AND {1} <= %({0}_b)s)'
            cmd = cmd.format(vkey, sqlfield)
            
            data[vkey] = value[0]
            data[vkey+'_b'] = value[1]
            return cmd, data
        
        # generate a contain query
        elif op in (Q.Op.Contains, Q.Op.DoesNotContain):
            if caseSensitive:
                sqlop = self.command(u'ContainsSensitive')
            else:
                sqlop = self.command(u'ContainsInsensitive')
            
            # perform binary comparison
            if column.isInteger() and not 'AsString' in functions:
                if op == Q.Op.Contains:
                    cmd = u'({0} & %({2})s) != 0'
                else:
                    cmd = u'({0} & %({2})s) == 0'
            else:
                if op == Q.Op.Contains:
                    cmd = u'{0} {1} %({2})s'
                else:
                    cmd = u'{0} NOT {1} %({2})s'
                
                native = projex.text.decoded(value)
                keysets = [(u'_', ur'\_'),
                           (ur'%', ur'\%'),
                           (ur'*', ur'%')]
                
                for old_key, new_key in keysets:
                    native = native.replace(old_key, new_key)
                
                value = u'%{0}%'.format(native)
            
            data[vkey] = value
            return cmd.format(sqlfield, sqlop, vkey), data
        
        # generate a startswith/endswith query
        elif op in (Q.Op.Startswith, Q.Op.Endswith):
            native = projex.text.decoded(value)
            if op == Q.Op.Startswith:
                value = u'^{0}.*$'.format(native)
            else:
                value = u'^.*{0}$'.format(native)
            
            sqlop = self.command('Matches')
            data[vkey] = value
            return u'{0} {1} %({2})s'.format(sqlfield, sqlop, vkey), data
        
        # generate a options query
        elif op in (Q.Op.IsIn, Q.Op.IsNotIn):
            value = tuple(value)
            if not value:
                return u'', {}
        
        # generate a basic query
        sqlop = self.command(Q.Op[op])
        if not sqlop:
            raise errors.DatabaseError('Unknown query operator: %s', op)
        
        data[vkey] = value
        return u'{0} {1} %({2})s'.format(sqlfield, sqlop, vkey), data
    
    def setSqltype(self, sqltype):
        """
        Sets the SQL type associated with this engine.
        
        :param      sqltype | <str>
        """
        self._sqltype = sqltype
        
    def sqltype(self):
        """
        Returns the SQL type associated with this engine.
        
        :return     <str>
        """
        return self._sqltype
    
#----------------------------------------------------------------------

class SqlSchemaEngine(SchemaEngine):
    def __init__(self, backend, wrapper=u'`', cmds=None):
        super(SqlSchemaEngine, self).__init__(backend, wrapper=wrapper)
        
        # define sql commands
        if cmds is not None:
            for key, cmd in cmds.items():
                self.setCommand(key, cmd)
        
    def alterCommand(self, schema, columns=None, ignore=None):
        """
        Generates the alter table command.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        # load the columns
        if columns is None:
            columns = schema.columns(recurse=False,
                                     includeProxies=False,
                                     includeJoined=False,
                                     includeAggregates=False)
        
        # generate the alter command
        cols = []
        db = self.backend().database()
        columns.sort(key = lambda x: x.name())
        for col in columns:
            engine = col.engine(db)
            for lang in col.languages():
                fname = col.fieldName(lang)
                if ignore is not None and fname in ignore:
                    continue
                
                ignore.append(fname)
                cols.append(engine.addCommand(col, lang))
        
        # generate options
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        opts['columns'] = u'\n\t' + u',\n\t'.join(cols)
        
        return self.command('alter').format(**opts), {}
    
    def countCommand(self, schemas, **options):
        """
        Returns the command that will be used to calculate the count that
        will be returned for the given query options.
        
        :param      schemas   | [<orb.TableSchema>, ..]
                    **options | database options
        
        :return     <str> sql, <dict> data
        """
        # include any required columns
        colnames = [schemas[0].primaryColumns()[0].name()]
        
        if 'where' in options:
            where = options['where']
        elif 'lookup' in options:
            where = options['lookup'].where
        else:
            where = None
        
        # join any columns from the query
        if where:
            for column in where.columns(schemas[0]):
                if column.isMemberOf(schemas):
                    colnames.append(column.name())
        
        # generate the options
        if 'lookup' in options:
            options['lookup'].columns = colnames
        elif not 'columns' in options:
            options['columns'] = colnames
        
        selcmd, data = self.selectCommand(schemas, **options)
        
        opts = {}
        opts['select_command'] = selcmd.rstrip(';')
        
        return self.command('count').format(**opts), data
    
    def columnsCommand(self, schema):
        """
        Returns the command for the inputed schema to lookup its
        columns from the database.
        
        :return     <str> sql, <dict> data
        """
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        data = {}
        data['table'] = schema.tableName()
        return self.command('existing_columns').format(**opts), data
    
    def createCommand(self, schema):
        """
        Generates the table creation command.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        # generate the column query command
        cols = []
        db = self.backend().database()
        columns = schema.columns(recurse=False,
                                 includeProxies=False,
                                 includeJoined=False,
                                 includeAggregates=False)
        columns.sort(key=lambda x: x.name())
        for col in columns:
            col_engine = col.engine(db)
            
            for language in col.languages():
                cols.append(col_engine.createCommand(col, language))
        
        # generate the constraints command
        constraints = []
        if not schema.inherits():
            pcols = schema.primaryColumns()
            pkeys = map(lambda x: self.wrapString(x.fieldName()), pcols)
            
            cmd = self.command('constraint_pkey')
            if cmd:
                opts = {}
                opts['table']   = self.wrapString(schema.tableName() + '_pkey')
                opts['columns'] = u','.join(pkeys)
                constraints.append(cmd.format(**opts))
        
        # generate options
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        if cols:
            opts['columns'] = u'\n\t' + u',\n\t'.join(cols)
        else:
            opts['columns'] = u''
        
        if constraints:
            opts['constraints'] = u',\n\t' + u',\n\t'.join(constraints)
        else:
            opts['constraints'] = u''
        opts['username'] = self.wrapString(db.username())
        
        if schema.inherits():
            inherited_schema = schema.ancestor()
            if not inherited_schema:
                raise errors.TableNotFoundError(schema.inherits())
            
            opts['inherit_id'] = self.wrapString(self.command('inherit_id'))
            opts['inherits'] = self.wrapString(inherited_schema.tableName())
            
            if opts['columns']:
                opts['columns'] = u',' + opts['columns']
            
            return self.command('create_inherited').format(**opts), {}
        else:
            return self.command('create').format(**opts), {}
    
    def existsCommand(self, schema):
        """
        Returns the command that will determine whether or not the schema
        exists in the database.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        data = {}
        data['table'] = schema.tableName()
        return self.command('exists').format(**opts), data
    
    def disableInternalsCommand(self, schema):
        """
        Generates the disable internals command for this schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        
        return self.command('disable_table_internals').format(**opts), {}
    
    def enableInternalsCommand(self, schema):
        """
        Generates the enable internals command for this schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        
        return self.command('enable_table_internals').format(**opts), {}
    
    def insertCommand(self,
                      schema,
                      records,
                      columns=None,
                      autoincrement=True,
                      first=True,
                      setup=None):
        """
        Generates the insertion command for this engine.
        
        :param      schema  | <orb.TableSchema>
                    records | [<orb.Table>, ..]
        
        :return     <str> command, <dict> data
        """
        # generate the values
        if setup is None:
            setup = {}
        
        fields = []
        values = []
        data = {}
        engines = {}
        schema_columns = []
        colnames = []
        cmd = u''
        is_base = True
        
        # map the column and field information
        db = self.backend().database()
        is_oo = self.backend().isObjectOriented()
        
        # insert ancestor records
        if not is_oo:
            ancest = schema.ancestor()
            if ancest:
                is_base = False
                cmd, data = self.insertCommand(ancest,
                                               records,
                                               columns=columns,
                                               autoincrement=autoincrement,
                                               first=False,
                                               setup=setup)
                
            elif autoincrement and not first:
                if setup.get(schema, {}).get('next_id') is None:
                    next_id_cmd = self.command('next_id')
                    opts = {}
                    opts['table'] = self.wrapString(schema.tableName())
                    
                    result, _ = self.backend().execute(next_id_cmd.format(**opts))
                    setup.setdefault(schema, {})
                    if result:
                        setup[schema]['next_id'] = result[0]['next_id']
                    else:
                        setup[schema]['next_id'] = 1
                
                for r, record in enumerate(records):
                    if type(record) == dict:
                        record['_id'] = setup[schema]['next_id']
                    else:
                        rdata = {'_id': setup[schema]['next_id']}
                        record._updateFromDatabase(rdata)
                    setup[schema]['next_id'] += 1
                
                autoincrement=False
        
        # insert individual column information
        for column in schema.columns(recurse=is_oo,
                                     includeProxies=False,
                                     includeJoined=False,
                                     includeAggregates=False):
            
            # ignore auto-incrementing columns
            if autoincrement and column.testFlag(Column.Flags.AutoIncrement):
                continue
            
            colname = column.name()
            if columns is None or colname in columns:
                for field in column.fieldNames():
                    fields.append(field)
                    colnames.append(colname)
                    schema_columns.append(column)
                    engines[colname] = column.engine(db)
        
        if not fields:
            return u'', {}
        
        # enumerates the records
        for r, record in enumerate(records):
            if Table.recordcheck(record):
                record = record.recordValues(autoInflate=False, language='all')
            
            field_values = []
            for i, colname in enumerate(colnames):
                column = schema_columns[i]
                if autoincrement and column.testFlag(Column.Flags.AutoIncrement):
                    continue
                
                field = fields[i]
                value = record.get(colname)
                
                base, lang = column.fieldInfo(field)
                if lang and type(value) == dict:
                    value = value[lang]
                
                key = self.genkey(field)
                field_values.append(u'%({0})s'.format(key))
                
                data[key] = engines[colname].wrap(column, value)
            
            if not is_base:
                key = self.genkey('__inherits__')
                if not '__inherits__' in fields:
                    fields.append('__inherits__')
                
                field_values.append(u'%({0})s'.format(key))
                data[key] = record.get('_id')
            
            values.append(u'(' + u','.join(field_values) + u')')
        
        # generate options
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        opts['columns'] = u','.join(map(self.wrapString, fields))
        opts['values'] = u',\n    '.join(values)
        
        out_cmd = self.command('insert').format(**opts)
        if cmd:
            out_cmd = cmd + u'\n' + out_cmd
        
        return out_cmd, data
    
    def insertedCommand(self, schema, count=1):
        """
        Returns the last inserted rows from the schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> sql, <dict> data
        """
        if self.backend().isObjectOriented() or not schema.inherits():
            pcols = schema.primaryColumns()
            if not pcols:
                raise errors.PrimaryKeyNotDefinedError()
            pcol = pcols[0].fieldName()
        
        elif schema.inherits():
            pcol = '__inherits__'
        
        # generate options for command
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        opts['column'] = self.wrapString(pcol)
        opts['count'] = count
        
        return self.command('inserted_keys').format(**opts), {}
    
    def joinTraversal(self, schemas, column, traverse, setup=None):
        """
        Joins the schemas from a traversal together for the setup.
        
        :param      schemas  | [<orb.TableSchema>, ..]
                    column   | <orb.Column> | source column
                    traverse | [<orb.TableSchema>, ..]
                    setup    | <dict> || None
        """
        if not (traverse and schemas):
            return
        
        if setup is None:
            setup = {}
        
        innerjoin = self.command('inner_join')
        is_oo = self.backend().isObjectOriented()
        path = []
        traverse.append(column)
        
        last_key = self.wrapString(traverse[0].schema().tableName(),
                                   traverse[0].fieldName())
                                   
        for curr in traverse[1:]:
            if is_oo:
                cschema = curr.firstMemberSchema(schemas)
            else:
                cschema = curr.schema()
            
            # update the nickname
            nickname = self.nextNickname(cschema.tableName(), setup)
            pkeycols = cschema.primaryColumns()
            if not is_oo:
                curr_key = self.wrapString(nickname, '__inherits__')
            elif len(pkeycols) == 1:
                curr_key = self.wrapString(nickname, pkeycols[0].fieldName())
            else:
                pdata = []
                for pkeycol in pkeycols:
                    pdata.append(self.wrapString(nickname, pkeycol.fieldName()))
                curr_key = u'('+u','.join(pdata)+u')'
            
            opts = {}
            opts['table'] = self.wrapString(cschema.tableName())
            opts['nickname'] = nickname
            opts['where'] = u'{0}={1}'.format(last_key, curr_key)
            joincmd = innerjoin.format(**opts)
            
            setup.setdefault('joins', [])
            setup.setdefault('join_columns', [])
            setup.setdefault('join_schemas', [])
            setup.setdefault('join_nicknames', [])
            
            setup['joins'].append(u'\n'+joincmd)
            setup['join_columns'].append((last_key, curr_key))
            setup['join_schemas'].append(cschema)
            setup['join_nicknames'].append(nickname)
            
            last_key = self.wrapString(nickname, curr.fieldName())
    
    def queryCommand(self, schemas, query, setup=None):
        """
        Generates the query command for the given information.
        
        :param      schemas | <orb.TableSchema>
                    query   | <orb.Query>
        
        :return     <str> command, <dict> data
        """
        if setup is None:
            setup = {}
        
        commands    = []
        traverse    = []
        data        = {}
        db          = self.backend().database()
        
        column      = query.column(schemas[0], traversal=traverse, db=db)
        language    = query.language()
        op          = query.operatorType()
        value       = query.value()
        is_oo       = self.backend().isObjectOriented()
        innerjoin   = self.command('inner_join')
        
        # add traversal paths to the command
        self.clearNicknames(setup)
        self.joinTraversal(schemas, column, traverse, setup)
        
        # determine if this query is a join query
        qschemas = query.schemas()
        if len(qschemas) > 1:
            joined = set(qschemas).difference(schemas+setup['join_schemas'])
            
            if joined:
                qschema = list(joined)[0]
                qwhere, _, qdata = self.whereCommand(schemas + [qschema],
                                                  query,
                                                  setup)
                
                if qwhere and \
                   not qschema in setup['join_query_order'] and \
                   not qschema in setup['orig_schemas']:
                    setup['join_schemas'].append(qschema)
                    setup['join_nicknames'].append(qschema.tableName())
                    setup['join_query_order'].append(qschema)
                    setup['join_queries'].setdefault(qschema, [])
                    setup['join_queries'][qschema].append(qwhere)
                    setup['data'].update(qdata)
                    
                    return u'', {}
        
        if not column:
            table = schemas[0].name()
            raise errors.ColumnNotFoundWarning(query.columnName(), table)
        
        # lookup primary key information
        if type(column) == tuple:
            if not type(value) in (list, tuple):
                value = (value,)
            
            if not len(column) == len(value):
                raise errors.InvalidQueryError(query)
        else:
            column = (column,)
            value  = (value,)
        
        if query.isOffset():
            offset, offset_data = self.queryOffset(schemas, query, setup)
            data.update(offset_data)
        else:
            offset = u''
        
        for column, value in dict(zip(column, value)).items():
            # process column specific information
            engine = column.engine(db)
            
            if is_oo:
                schema = column.firstMemberSchema(schemas)
            else:
                schema = column.schema()
            
            tableName = self.currentNickname(schema.tableName(), setup)
            
            if language in ('all', 'any'):
                langs = column.languages()
            elif language is None:
                langs = [orb.Orb.instance().language()]
            else:
                langs = [language]
            
            lcmds = []
            ldata = {}
            
            for lang in langs:
                field = column.fieldName(lang)
                sqlfield = self.wrapString(tableName, field)
                
                #-----------------------------------------------------------------
                # pre-process value options
                #-----------------------------------------------------------------
                # ignore query
                if Q.typecheck(value):
                    pass
                
                # lookup all instances of a record
                elif value == Q.ALL:
                    continue
                
                # lookup all NOT NULL instances of a record
                elif value == Q.NOT_EMPTY or \
                     (value == None and op == Q.Op.IsNot):
                    commands.append(u'{0} IS NOT NULL'.format(sqlfield))
                    continue
                
                # lookup all NULL instances
                elif value == Q.EMPTY or (value == None and op == Q.Op.Is):
                    commands.append(u'{0} IS NULL'.format(sqlfield))
                    continue
                
                # process the value
                else:
                    value = engine.wrap(column, value)
                
                vcmd, vdata = engine.queryCommand(schema,
                                            column,
                                            op,
                                            value,
                                            offset=offset,
                                            caseSensitive=query.caseSensitive(),
                                            functions=query.functionNames(),
                                            setup=setup,
                                            language=lang)
                
                if vcmd:
                    lcmds.append(vcmd)
                    ldata.update(vdata)
            
            if len(lcmds) > 1:
                joiner = u' OR ' if language == 'any' else u' AND '
                commands.append(u'({0})'.format(joiner.join(lcmds)))
                data.update(ldata)
            
            elif lcmds:
                commands.append(lcmds[0])
                data.update(ldata)
    
        # strip out any query reference information
        cmd = u' AND '.join(commands)
        for key, val in data.items():
            if Q.typecheck(val):
                qcmd, qdata = self.queryValue(schemas, val, setup)
                cmd = cmd.replace(u'%({0})s'.format(key), qcmd)
                data.pop(key)
                data.update(qdata)
        
        return cmd, data
        
    def removeCommand(self, schema, records):
        """
        Generates the command for removing the inputed query from the
        database.
        
        :param      schema  | <orb.TableSchema>
                    records | <orb.RecordSet> || [<orb.Table>, ..]
        
        :return     <str> command, <dict> data
        """
        # generate a list of removal commands
        commands = []
        data = {}
        setup = {}
        cmd = self.command('delete')
        
        # generate command options
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        opts['join'] = u''
        opts['where'] = u''
        
        # remove record sets by using the where clause
        if RecordSet.typecheck(records):
            wcmd, _, wdata = self.whereCommand([schema],
                                                records.query(),
                                                setup=setup)
            
            if 'join_schemas' in setup:
                join_cmd = self.command('delete_join')
                joined = setup['join_schemas']
                nicknames = setup['join_nicknames']
                
                names = []
                for i, jschema in enumerate(joined):
                    name_opts = [jschema.tableName(), nicknames[i]]
                    name_opts = map(self.wrapString, name_opts)
                    names.append(u'{0} AS {1}'.format(*name_opts))
                
                opts['join'] = u' ' + join_cmd.format(tables=u','.join(names))
                
                cols = setup['join_columns']
                equals = [u'{0}={1}'.format(a, b) for a, b in cols]
                where_cmd = u' AND '.join(equals)
                
                if wcmd:
                    wcmd = u'{0} AND ({1})'.format(where_cmd, wcmd)
                else:
                    wcmd = where_cmd
            
            if wcmd:
                opts['where'] = u' WHERE {0}'.format(wcmd)
            
            data.update(wdata)
            commands.append(cmd.format(**opts))
        
        # remove direct records by their primary key
        else:
            is_oo = self.backend().isObjectOriented()
            pcols = schema.primaryColumns()
            pkeys = []
            
            # remove from non-object-oriented databases
            if not is_oo and schema.inherits():
                ancest = schema.ancestor()
                if ancest:
                    base_cmd, base_data = self.removeCommand(ancest, records)
                    commands.append(base_cmd)
                    data.update(base_data)
                
                pcolnames = self.wrapString('__inherits__')
            
            # remove base records
            elif len(pcols) == 1:
                pcolnames = self.wrapString(pcols[0].schema().tableName(),
                                            pcols[0].fieldName())
            
            else:
                mapper = lambda x: self.wrapString(x.schema().tableName(), 
                                                   x.fieldName())
                pcolnames = u'('+u','.join(map(mapper, pcols)) + u')'
            
            # collect the primary keys for removal
            for record in records:
                if Table.recordcheck(record):
                    pkey = record.primaryKey()
                    if pkey is None:
                        continue
                else:
                    pkey = record
                
                datakey = self.genkey('pkey')
                data[datakey] = pkey
                pkeys.append(u'%({0})s'.format(datakey))
            
            # batch out 100 records to remove at a time
            for batch in projex.iters.batch(pkeys, 100):
                vals = (pcolnames, u','.join(batch))
                
                opts['where'] = u'WHERE {0} IN ({1})'.format(*vals)
                commands.append(cmd.format(**opts))
        
        return u'\n'.join(commands), data

    def selectCommand(self, schemas, **options):
        """
        Generates the selection command for this engine.
        
        :param      schemas | [<orb.TableSchema>, ..]
        
        :return     <str> command, <dict> data
        """
        # initialize data collections
        setup = options.get('setup', {})
        data  = setup.get('data', {})
        
        fields = []
        
        setup.setdefault('data', data)
        setup.setdefault('aliases', {})
        setup.setdefault('orig_schemas', schemas[:])
        setup.setdefault('where', u'')
        setup.setdefault('having', u'')
        setup.setdefault('join_queries', {})
        setup.setdefault('join_query_order', [])
        setup.setdefault('lookup', [])
        setup.setdefault('column_aliases', {})
        setup.setdefault('group_by', [])
        setup.setdefault('joins', [])
        setup.setdefault('join_schemas', [])
        setup.setdefault('join_nicknames', [])
        
        # use provided options
        datamapper  = options.get('datamapper', {})
        columns     = options.get('columns')
        lookup      = options.get('lookup', orb.LookupOptions(**options))
        is_oo       = self.backend().isObjectOriented()
        schemas     = schemas[:]
        aggregate_aliases = {}
        aggregate_cache = {}
        
        innerjoin   = self.command('inner_join')
        outerjoin   = self.command('outer_join')
        aggrjoin    = self.command('aggregate')
        
        # create the grouping keys
        # include any inheritance
        if not is_oo:
            bases = []
            pkey = self.command('primary_id')
            inherit = self.command('inherit_id')
            for schema in schemas[:]:
                ancestry = schema.ancestry()
                ancestry.append(schema)
                for i in range(1, len(ancestry)):
                    tableName = ancestry[i-1].tableName()
                    if i == 1:
                        a = self.wrapString(tableName, pkey)
                    else:
                        a = self.wrapString(tableName, inherit)
                    
                    b = self.wrapString(ancestry[i].tableName(), inherit)
                    
                    opts = {}
                    opts['table'] = self.wrapString(tableName)
                    opts['nickname'] = self.wrapString(tableName)
                    opts['where'] = u'{0}={1}'.format(a, b)
                    
                    setup['joins'].append(u'\n'+innerjoin.format(**opts))
                    if not ancestry[i-1] in setup['join_schemas']:
                        aschema = ancestry[i-1]
                        setup['join_schemas'].append(aschema)
                        setup['join_nicknames'].append(aschema.tableName())
        
        # determine which coluns to lookup
        if columns is None:
            columns = [c for s in schemas \
                         for c in s.columns(includeProxies=False)]
            if lookup.columns:
                columns = filter(lambda x: x.name() in lookup.columns, columns)
                
        # lookup joined & aggregate columns
        use_group_by = False
        for column in columns:
            joiner    = column.joiner()
            aggregate = column.aggregate()
            
            # include joiner
            if joiner and not lookup.ignoreJoined:
                adv_schema = joiner[0].schema()
                adv_where = joiner[1]
            
            # include aggregate
            elif aggregate and not lookup.ignoreAggregates:
                adv_schema = aggregate.table().schema()
                adv_where = aggregate.lookupOptions().where
            
            # ignore this column
            else:
                continue
            
            aggr_key = (adv_schema.name(), hash(adv_where))
            if aggr_key in aggregate_cache:
                adv_key = aggregate_cache[aggr_key]
                setup['column_aliases'][column] = adv_key
                continue
            else:
                adv_key = column.fieldName() + '_aggr'
                aggregate_cache[aggr_key] = adv_key
                setup['column_aliases'][column] = adv_key
            
            use_group_by = True
            where_cmd, _, where_data = self.whereCommand([adv_schema],
                                                      adv_where,
                                                      setup)
            
            where_cmd = where_cmd.replace(adv_schema.tableName(), adv_key)
            
            opts = {}
            opts['table'] = self.wrapString(adv_schema.tableName())
            opts['aggregate'] = self.wrapString(adv_key)
            opts['where'] = where_cmd
            
            setup['joins'].append(u'\n'+aggrjoin.format(**opts))
            data.update(where_data)
            
        # generare where SQL
        if lookup.where is not None:
            qwhere, qhaving, qdata = self.whereCommand(schemas,
                                            lookup.where,
                                            setup)
            
            # if the where value could not be generated, then ignore it
            if not qwhere:
                return u'', {}
            
            # add any traversal queries
            for schema in setup['join_query_order']:
                queries = setup['join_queries'][schema]
                tableName = schema.tableName()
                
                opts = {}
                opts['table'] = self.wrapString(tableName)
                opts['nickname'] = self.currentNickname(tableName, setup)
                opts['where'] = u' AND '.join(queries)
                
                setup['joins'].append('\n'+innerjoin.format(**opts))
            
            if qwhere:
                setup['where'] = u'\nWHERE {0}'.format(qwhere)
                data.update(qdata)
            
            if qhaving:
                setup['having'] = u'\nHAVING {0}'.format(qhaving)
                data.update(qdata)
            
        # generate default order
        if lookup.order is None:
            lookup.order = schemas[0].defaultOrder()
        
        # generate order SQL
        if lookup.order is not None:
            order_cmds = []
            for colname, direction in lookup.order:
                # look up the first matching column
                scolumn = None
                straverse = []
                for schema in schemas:
                    traverse = []
                    column = schema.column(colname, traversal=traverse)
                    if column:
                        scolumn = column
                        straverse = traverse
                        break
                
                # add the schema sorted column to the system
                if not scolumn:
                    continue
                
                # join in the columns if necessary
                self.clearNicknames(setup)
                self.joinTraversal(schemas, scolumn, straverse, setup)
                
                if is_oo:
                    schema = scolumn.firstMemberSchema(schemas)
                else:
                    schema = scolumn.schema()
                
                nickname = self.currentNickname(schema.tableName(), setup)
                
                if scolumn.isAggregate() or scolumn.isJoined():
                    key = nickname + '_' + scolumn.fieldName()
                    key = self.wrapString(key)
                else:
                    key = self.wrapString(nickname, scolumn.fieldName())
                    setup['group_by'].append(key)
                
                if lookup.distinct and not key in fields:
                    fields.append(key)
                
                order_cmds.append(u'{0} {1}'.format(key, direction.upper()))
            
            if order_cmds:
                cmd = u'\nORDER BY {0}'.format(u','.join(order_cmds))
                setup['lookup'].append(cmd)
        
        # generate limit SQL
        if lookup.limit is not None:
            setup['lookup'].append(u'\nLIMIT {0}'.format(lookup.limit))
        
        # generate offset SQL
        if lookup.start is not None:
            setup['lookup'].append(u'\nOFFSET {0}'.format(lookup.start))
        
        # define selection options
        select_options = []
        if lookup.distinct:
            select_options.append(u'DISTINCT')
        
        # generate the fields to lookup
        for column in columns:
            for field in column.fieldNames():
                if is_oo:
                    schema = column.firstMemberSchema(schemas)
                else:
                    schema = column.schema()
                
                use_alias = False
                
                # include joins for this query
                if column.isJoined():
                    if not column in setup['column_aliases']:
                        continue
                    
                    src = column.joiner()[0]
                    src_table = setup['column_aliases'][column]
                    src_field = src.fieldName()
                    src = self.wrapString(src_table, src_field)
                    setup['group_by'].append(src)
                
                # include aggregates for this query
                elif column.isAggregate():
                    if not column in setup['column_aliases']:
                        continue
                    
                    aggr = column.aggregate()
                    src_table = setup['column_aliases'][column]
                    
                    opts = {}
                    opts['table'] = self.wrapString(src_table)
                    if schema.inherits() and not is_oo:
                        opts['colname'] = self.wrapString('__inherits__')
                    else:
                        pcol = schema.primaryColumns()[0]
                        opts['colname'] = self.wrapString(pcol.fieldName())
                    
                    cmd = self.command('aggr_{0}'.format(aggr.type()))
                    src = cmd.format(**opts)
                    use_alias = True
                
                # include regular columns
                else:
                    src_table = schema.tableName()
                    src_field = field
                    src = self.wrapString(src_table, src_field)
                    setup['group_by'].append(src)
                
                target_key = schema.tableName() + '_' + field
                
                datamapper[target_key] = column
                opts = (src, self.wrapString(target_key))
                fields.append(u'{0} AS {1}'.format(*opts))
        
        # generate the SQL table names
        tables = map(lambda x: self.wrapString(x.tableName()), schemas)
        
        # generate options
        opts = {}
        opts['select_options'] = u' '.join(select_options)
        opts['tables'] = u','.join(tables)
        opts['columns'] = u','.join(fields)
        opts['joins'] = u''.join(setup['joins'])
        opts['where'] = setup['where']
        opts['having'] = setup['having']
        opts['lookup_options'] = u''.join(setup['lookup'])
        
        if use_group_by:
            opts['group_by'] = u'\nGROUP BY ' + ','.join(setup['group_by'])
        else:
            opts['group_by'] = u''
        
        return self.command('select').format(**opts), data
    
    def updateCommand(self, schema, records, columns=None):
        """
        Updates the records for the given schema in the database.
        
        :param      schema  | <orb.TableSchema>
                    records | [<orb.Table>, ..]
        
        :return     <str> command, <data> dict
        """
        pcols = schema.primaryColumns()
        pkeys = map(lambda x: x.primaryKey(), records)
        is_oo = self.backend().isObjectOriented()
        
        data  = {}
        commands = []
        db = self.backend().database()
        updatecmd = self.command('update')
        
        for r, record in enumerate(records):
            if not record.isRecord():
                continue
            
            changes = record.changeset(columns=columns, includeProxies=False)
            
            if not changes:
                continue
            
            values = []
            where = []
            pkey = pkeys[r]
            if not type(pkey) in (list, tuple):
                pkey = (pkey,)
            
            # generate the where command
            if len(pkey) != len(pcols):
                raise errors.DatabaseError('Invalid primary key: %s' % pkey)
            
            elif len(pkey) == 1:
                if is_oo or not schema.inherits():
                    colcmd = u'{0}=%({1})s'
                    tname = pcols[0].schema().tableName()
                    fname = pcols[0].fieldName()
                    field = self.wrapString(tname, fname)
                    datakey = self.genkey(fname)
                else:
                    colcmd = u'{0}=%({1})s'
                    field = self.wrapString(self.command('inherit_id'))
                    datakey = self.genkey('inherits')
                
                cmd = colcmd.format(field, datakey)
                data[datakey] = pkey[0]
                where.append(cmd)
            
            else:
                for i, pcol in enumerate(pcols):
                    colcmd = u'{0}=%({1})s'
                    tname = pcol.schema().tableName()
                    fname = pcol.fieldName()
                    datakey = self.genkey(fname)
                    cmd = colcmd.format(self.wrapString(tname, fname), datakey)
                    data[datakey] = pkey[i]
                    where.append(cmd)
            
            # generate the setting command
            for colname, change in changes.items():
                column = schema.column(colname, recurse=is_oo)
                if not column:
                    continue
                
                engine = column.engine(db)
                if column.isTranslatable():
                    for lang, value in change[1].items():
                        newval = engine.wrap(column, value)
                        
                        field = self.wrapString(column.fieldName(lang))
                        
                        datakey = self.genkey('update_' + column.fieldName(lang))
                        data[datakey] = newval
                        values.append(u'{0}=%({1})s'.format(field, datakey))
                else:
                    newval = engine.wrap(column, change[1])
                    
                    field = self.wrapString(column.fieldName())
                    
                    datakey = self.genkey('update_' + column.fieldName())
                    data[datakey] = newval
                    values.append(u'{0}=%({1})s'.format(field, datakey))
            
            if values:
                # generate formatting options
                opts = {}
                opts['table'] = self.wrapString(schema.tableName())
                opts['values'] = u', '.join(values)
                opts['where'] = u' AND '.join(where)
                
                commands.append(updatecmd.format(**opts))
        
        return u'\n'.join(commands), data

    def truncateCommand(self, schema):
        """
        Generates the truncation command for this schema.
        
        :param      schema | <orb.TableSchema>
        
        :return     <str> command, <dict> data
        """
        opts = {}
        opts['table'] = self.wrapString(schema.tableName())
        
        return self.command('truncate').format(**opts), {}

#----------------------------------------------------------------------

class SqlBase(Connection):
    """ 
    Creates a SQL based backend connection type for handling database
    connections to different SQL based databases.  This class can be subclassed
    to define different SQL connections.
    """
    
    def __init__(self,
                 database,
                 stringWrapper="`",
                 schemaCommands=None,
                 columnCommands=None,
                 typeMap=None,
                 schemaEngine=SqlSchemaEngine,
                 columnEngine=SqlColumnEngine):
        
        super(SqlBase, self).__init__(database)
        
        # define custom properties
        self._insertBatchSize = 500
        self._thread_dbs = {}      # thread id, database conneciton
        
        # set standard properties
        self.setThreadEnabled(True)
        
        if schemaCommands is None:
            schemaCommands = DEFAULT_SCHEMA_CMDS
        if columnCommands is None:
            columnCommands = DEFAULT_COLUMN_CMDS
        if typeMap is None:
            typeMap = DEFAULT_TYPE_MAP
        
        # define default wrappers
        self.setEngine(schemaEngine(self,
                                    wrapper=stringWrapper,
                                    cmds=schemaCommands))
        
        for typ, sqltype in typeMap.items():
            col_engine = columnEngine(self,
                                      sqltype,
                                      wrapper=stringWrapper,
                                      cmds=columnCommands)
            self.setColumnEngine(typ, col_engine)
        
        # add any custom engines
        plug = type(self).__name__
        for typ, engine_type in orb.Orb.instance().customEngines(plug):
            col_engine = engine_type(self,
                                     wrapper=stringWrapper,
                                     cmds=columnCommands)
            self.setColumnEngine(typ, col_engine)

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
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        return None
    
    @abstractmethod()
    def _interrupt(self, threadId, backendDb):
        """
        Interrupts the given backend database connection from a separate thread.
        
        :param      threadId   | <int>
                    backendDb | <variant> | backend specific database.
        """
        pass

    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------

    def backendDb(self):
        """
        Returns the sqlite database for the current thread.
        
        :return     <variant> || None
        """
        tid = threading.current_thread().ident
        return self._thread_dbs.get(tid)
    
    def close(self):
        """
        Closes the connection to the datbaase for this connection.
        
        :return     <bool> closed
        """
        cid = threading.current_thread().ident
        for tid, thread_db in self._thread_dbs.items():
            if tid == cid:
                thread_db.close()
            else:
                self._interrupt(tid, thread_db)
        
        self._thread_dbs.clear()
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
        if Table.typecheck(table_or_join):
            schemas = [table_or_join.schema()]
        else:
            schemas = map(lambda x: x.schema(), table_or_join.tables())
        
        engine = self.engine()
        sql, data = engine.countCommand(schemas, lookup=lookup)
        
        results, _ = self.execute(sql,
                                  data,
                                  mapper=lambda x: dict(x)['count'],
                                  autoCommit=False)
        return sum(results)
    
    def commit(self):
        """
        Commits the changes to the current database connection.
        
        :return     <bool> success
        """
        if not (self.isConnected() and self.commitEnabled()):
            return False
        
        if Transaction.current():
            Transaction.current().setDirty(self)
        else:
            self.backendDb().commit()
        return True
    
    def createTable(self, schema, options):
        """
        Creates a new table in the database based cff the inputed
        schema information.  If the dryRun flag is specified, then
        the SQL will only be logged to the current logger, and not
        actually executed in the database.
        
        :param      schema    | <orb.TableSchema>
                    options   | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        if schema.isAbstract():
            name = schema.name()
            logger.debug('%s is an abstract table, not creating' % name)
            return False
        
        engine = self.engine()
        sql, data = engine.createCommand(schema)
        
        if not options.dryRun:
            self.execute(sql)
        else:
            logger.info(u'\n'+command+u'\n')
        
        logger.info('Created %s table.' % schema.tableName())
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
        super(SqlBase, self).disableInternals()
        
        # disable the internal processes within the database for faster
        # insertion
        engine = self.engine()
        sql = engine.command('disable_internals')
        if sql:
            self.execute(sql, autoCommit=False)
    
    def distinct(self, table_or_join, lookup, options):
        """
        Returns a distinct set of results for the given information.
        
        :param      table_or_join | <subclass of orb.Table> || <orb.Join>
                    lookup        | <orb.LookupOptions>
                    options       | <orb.DatabaseOptions>
        
        :return     {<str> columnName: <list> value, ..}
        """
        if Table.typecheck(table_or_join):
            schemas = [table_or_join.schema()]
        else:
            schemas = map(lambda x:schema(), table_or_join.tables())
        
        lookup.distinct = True
        sql, data = self.engine().selectCommand(schemas, lookup=lookup)
        
        if not sql:
            db_results = {}
        else:
            db_results, _ = self.execute(sql, data, autoCommit=False)
        
        output = dict([(column, set()) for column in lookup.columns])
        for schema in schemas:
            for db_result in db_results:
                for colname in lookup.columns:
                    col = schema.column(colname)
                    if not col:
                        continue
                    
                    key = u'{0}_{1}'.format(col.schema().tableName(),
                                            col.fieldName())
                    output[col.name()].add(db_result.get(key))
        
        for key, value in output.items():
            output[key] = list(value)
        
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
        # enables the internal processes within the database for protected
        # insertions and changes
        engine = self.engine()
        sql = engine.command('enable_internals')
        if sql:
            self.execute(sql, autoCommit=False)
        
        super(SqlBase, self).enableInternals()
    
    def existingColumns(self, schema, namespace=None, mapper=None):
        """
        Looks up the existing columns from the database based on the
        inputed schema and namespace information.
        
        :param      schema      | <orb.TableSchema>
                    namespace   | <str> || None
        
        :return     [<str>, ..]
        """
        if mapper is None:
            mapper = lambda x: nativestring(x[0])
        engine = self.engine()
        sql, data = engine.columnsCommand(schema)
        return self.execute(sql, data, mapper=mapper, autoCommit=False)[0]
    
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
        
        if not self.open():
            raise errors.ConnectionError('Failed to open connection.',
                                         self.database())
        
        # when in debug mode, simply log the command to the logger
        elif self.database().commandsBlocked():
            logger.info(command)
            return [], rowcount
        
        # make sure this is a valid query
        elif u'__QUERY__UNDEFINED__' in command:
            logger.info('Query is undefined.')
            logger.debug(command)
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
                logger.debug('Query took: %s' % delta)
                raise
            
            # attempt to reconnect as long as we have enough retries left
            # otherwise raise the error
            except errors.ConnectionLostError, err:
                delta = datetime.datetime.now() - start
                logger.debug('Query took: %s' % delta)
                
                if i != (retries - 1):
                    time.sleep(0.25)
                    self.reconnect()
                else:
                    raise
            
            # handle any known a database errors with feedback information
            except errors.DatabaseError, err:
                delta = datetime.datetime.now() - start
                logger.debug('Query took: %s' % delta)\
                
                if self.isConnected():
                    if Transaction.current():
                        Transaction.current().rollback(err)
                    
                    try:
                        self.rollback()
                    except:
                        pass
                    
                    raise
                else:
                    raise
            
            # always raise any unknown issues for the developer
            except StandardError:
                delta = datetime.datetime.now() - start
                logger.debug('Query took: %s' % delta)
                raise
        
        delta = datetime.datetime.now() - start
        logger.debug('Query took: %s' % delta)
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
        if RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif Table.recordcheck(records):
            records = [records]
        
        # determine the proper records for insertion
        inserter = {}
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
            
            schema = record.schema()
            inserter.setdefault(schema, [])
            inserter[schema].append(record)
        
        cmds = []
        data = {}
        setup = {}
        
        autoinc = options.autoIncrement
        
        engine = self.engine()
        for schema, schema_records in inserter.items():
            if not schema_records:
                continue
            
            
            colcount = len(schema.columns())
            batchsize = self.insertBatchSize()
            size = batchsize / max(int(round(colcount/10.0)), 1)
            
            for batch in projex.iters.batch(schema_records, size):
                batch = list(batch)
                icmd, idata = engine.insertCommand(schema,
                                                   batch,
                                                   columns=lookup.columns,
                                                   autoincrement=autoinc,
                                                   setup=setup)
                cmds.append(icmd)
                data.update(idata)
            
            # for inherited schemas in non-OO tables, we'll define the
            # primary keys before insertion
            if autoinc and (not schema.inherits() or self.isObjectOriented()):
                cmd, dat = engine.insertedCommand(schema, count=len(batch))
                cmds.append(cmd)
                data.update(dat)
        
        if not cmds:
            return {}
        
        sql = u'\n'.join(cmds)
        results, _ = self.execute(sql, data, autoCommit=False)
        
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
        return self._insertBatchSize
    
    def interrupt(self, threadId=None):
        """
        Interrupts the access to the database for the given thread.
        
        :param      threadId | <int> || None
        """
        cid = threading.current_thread().ident
        if threadId is None:
            cid = threading.current_thread().ident
            for tid, thread_db in self._thread_dbs.items():
                if tid != cid:
                    thread_db.interrupt()
                    self._thread_dbs.pop(tid)
        else:
            thread_db = self._thread_dbs.get(threadId)
            if not thread_db:
                return
            
            if threadId == cid:
                thread_db.close()
            else:
                self._interrupt(threadId, thread_db)
            
            self._thread_dbs.pop(threadId)
    
    def isConnected(self):
        """
        Returns whether or not this conection is currently
        active.
        
        :return     <bool> connected
        """
        return self.backendDb() != None
    
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
                self._thread_dbs.pop(thread.ident, None)
        
        thread_db = self._thread_dbs.get(tid)
        
        # check to see if we already have a connection going
        if thread_db:
            return True
        
        # make sure we have a database assigned to this backend
        elif not self._database:
            raise errors.DatabaseNotFoundError()
        
        # open a new backend connection to the database for this thread
        backend_db = self._open(self._database)
        if backend_db:
            self._thread_dbs[tid] = backend_db
            Orb.instance().runCallback(CallbackType.ConnectionCreated,
                                       self._database)
        else:
            Orb.instance().runCallback(CallbackType.ConnectionFailed,
                                       self._database)
        
        return backend_db != None
    
    def reconnect(self):
        """
        Forces a reconnection to the database.
        """
        tid = threading.current_thread().ident
        db = self._thread_dbs.pop(tid, None)
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
        engine = self.engine()
        for table, queries in remove.items():
            for query in queries:
                records = table.select(where=query)
                sql, data = engine.removeCommand(table.schema(), records)
                if options.dryRun:
                    logger.info(sql)
                else:
                    count += self.execute(sql, data)[1]
        
        return count
    
    def rollback(self):
        """
        Rolls back changes to this database.
        """
        db = self.backendDb()
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
        if Table.typecheck(table_or_join):
            schemas = [table_or_join.schema()]
        else:
            schemas = [table.schema() for table in table_or_join.tables()]
        
        datamapper = {}
        engine = self.engine()
        sql, data = engine.selectCommand(schemas,
                                         lookup=lookup,
                                         datamapper=datamapper)
        
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
                column = datamapper.get(db_key)
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
        
        if Table.typecheck(table_or_join):
            return map(lambda x: x[0], output)
        return output
    
    def setInsertBatchSize(self, size):
        """
        Sets the maximum number of records that can be inserted for a single
        insert statement.
        
        :param      size | <int>
        """
        self._insertBatchSize = size
    
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
        engine = self.engine()
        sql, data = engine.existsCommand(schema)
        if not sql:
            return False
        
        results, _ = self.execute(sql, data, autoCommit=False)
        if results:
            return True
        return False
    
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
        if RecordSet.typecheck(records):
            records = list(records)
        
        # wrap the record in a list
        elif Table.recordcheck(records):
            records = [records]
        
        is_oo = self.isObjectOriented()
        updater = {}
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
                updater.setdefault(schema, [])
                updater[schema].append(record)
        
        if not updater:
            if len(records) > 1:
                return []
            else:
                return {}
        
        cmds = []
        data = {}
        
        engine = self.engine()
        for schema, schema_records in updater.items():
            icmd, idata = engine.updateCommand(schema,
                                               schema_records,
                                               columns=lookup.columns)
            cmds.append(icmd)
            data.update(idata)
        
        sql = u'\n'.join(cmds)
        results, _ = self.execute(sql, data, autoCommit=False)
        
        if not self.commit():
            if len(changes) == 1:
                return {}
            return []
        
        # update the values for the database
        for record in records:
            record._markAsLoaded(self.database(), columns=lookup.columns)
        
        if len(changes) == 1:
            return changes[0]
        return changes
    
    def updateTable(self, schema, options):
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
        
        :param      schema     | <orb.TableSchema>
                    options    | <orb.DatabaseOptions>
        
        :return     <bool> success
        """
        # determine the new columns
        existing = self.existingColumns(schema)
        missing  = schema.fieldNames(recurse=False,
                                     includeProxies=False,
                                     includeJoined=False,
                                     includeAggregates=False,
                                     ignore=existing)
        
        # if no columns are missing, return True to indicate the table is
        # up to date
        if not missing:
            return True
        
        logger.info('Updating {0}...'.format(schema.name()))
        
        columns = map(schema.column, missing)
        engine = self.engine()
        sql, data = engine.alterCommand(schema, columns, ignore=existing)
        if options.dryRun:
            logger.info(u'\n'+sql+u'\n')
        else:
            self.execute(sql, data)
            
            opts = (schema.name(),
                    u','.join([f for x in columns for f in x.fieldNames()]))
            logger.info(u'Updated {0} table: added {1}'.format(*opts))
        
        return True
