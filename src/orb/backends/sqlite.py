#!/usr/bin/python

""" Defines the backend connection class for SQLite databases. """

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
__depends__        = ['sqlite3']
__version_info__   = (0, 0, 0)
__version__        = '%i.%i.%i' % __version_info__

#------------------------------------------------------------------------------

import datetime
import logging
import os.path
import re
import threading
import weakref

import projex.text
import projex.errors

from projex.text import nativestring

from orb                 import errors, Orb
from orb.common          import ColumnType
from orb.column          import Column
from orb.connection      import Connection
from orb.transaction     import Transaction
from orb.table           import Table
from orb.join            import Join
from orb.query           import Query as Q
from orb.valuemapper     import ValueMapper

logger = logging.getLogger(__name__)

try:
    import sqlite3 as sqlite
    
except ImportError:
    text = 'For SQLite backend, ensure your python version supports sqlite3'
    deperr  = projex.errors.DependencyNotFoundError(text)
    logger.debug( deperr )
    sqlite = None

#------------------------------------------------------------------------------

FORMAT_EXPR = re.compile('(%\(([^\)]+)\)s)')

def matches(expr, item):
    """
    Generates a regular expression function.
    
    :param      expr | <str>
                item | <str>
    
    :return     <bool>
    """
    return re.match(expr, item) is not None

def does_not_match(expr, item):
    """
    Generates a negated regular expression function.
    
    :param      expr | <str>
                item | <str>
    
    :return     <bool>
    """
    return re.match(expr, item) is None

def dict_factory(cursor, row):
    """
    Converts the cursor information from a SQLite query to a dictionary.
    
    :param      cursor | <sqlite3.Cursor>
                row    | <sqlite3.Row>
    
    :return     {<str> column: <variant> value, ..}
    """
    out = {}
    for i, col in enumerate(cursor.description):
        out[col[0]] = row[i]
    return out

#----------------------------------------------------------------------

from orb.backends.sqlbase import SqlBase,\
                                 DEFAULT_SCHEMA_CMDS as SQL_SCHEMA_CMDS,\
                                 DEFAULT_COLUMN_CMDS as SQL_COLUMN_CMDS,\
                                 DEFAULT_TYPE_MAP as SQL_TYPE_MAP,\
                                 SqlColumnEngine,\
                                 SqlSchemaEngine

LITE_SCHEMA_CMDS = {
'constraint_pkey': '',
'exists':
    """
    SELECT tbl_name FROM sqlite_master
    WHERE type='table' AND tbl_name=%(table)s
    """,
'inner_join':
    """
    JOIN {table} AS {nickname} ON {where}
    """,
'existing_columns':
    """
    PRAGMA table_info({table})
    """,
'inserted_keys':
    """
    SELECT {column} FROM {table}
    WHERE {column}>=last_insert_rowid()-{count}+1
    ORDER BY {column} ASC;
    """,
'disable_internals': """
    PRAGMA foreign_keys=FALSE;
    PRAGMA count_changes=FALSE;
""",
'enable_internals': """
    PRAGMA foreign_keys=TRUE;
    PRAGMA count_changes=TRUE;
""",
'disable_table_internals': '',
'enable_table_internals': '',
'truncate':
    """
    DELETE FROM {table};
    """
}
DEFAULT_SCHEMA_CMDS = SQL_SCHEMA_CMDS.copy()
DEFAULT_SCHEMA_CMDS.update(LITE_SCHEMA_CMDS)

LITE_COLUMN_CMDS = {
'ContainsInsensitive':  'LIKE',
'Matches':              'REGEXP',
'DoesNotMatch':         'REGEXP',

Column.Flags.Required:      '',
Column.Flags.Unique:        '',
Column.Flags.AutoIncrement: '',

'AsString': "cast({0} as text)",
}
DEFAULT_COLUMN_CMDS = SQL_COLUMN_CMDS.copy()
DEFAULT_COLUMN_CMDS.update(LITE_COLUMN_CMDS)


LITE_TYPE_MAP = {
    ColumnType.Bool:        'INTEGER',
    
    ColumnType.Decimal:     'REAL',
    ColumnType.Double:      'REAL',
    ColumnType.Integer:     'INTEGER',
    ColumnType.Enum:        'INTEGER',
    ColumnType.BigInt:      'INTEGER',
    
    ColumnType.ForeignKey:  'INTEGER',
    
    ColumnType.Datetime:    'TEXT',
    ColumnType.Date:        'TEXT',
    ColumnType.Time:        'TEXT',
    ColumnType.Interval:    'TEXT',
    
    ColumnType.String:      'TEXT',
    ColumnType.Color:       'TEXT',
    ColumnType.Email:       'TEXT',
    ColumnType.Password:    'TEXT',
    ColumnType.Url:         'TEXT',
    ColumnType.Filepath:    'TEXT',
    ColumnType.Directory:   'TEXT',
    ColumnType.Text:        'TEXT',
    ColumnType.Xml:         'TEXT',
    ColumnType.Html:        'TEXT',
    
    ColumnType.ByteArray:   'TEXT',
    ColumnType.Pickle:      'BLOB',
    
}
DEFAULT_TYPE_MAP = SQL_TYPE_MAP.copy()
DEFAULT_TYPE_MAP.update(LITE_TYPE_MAP)

class SQLiteColumnEngine(SqlColumnEngine):
    def unwrap(self, column, value):
        """
        Restores the value from the database for the given column.
        
        :param      column | <orb.Column>
                    value  | <variant>
        """
        coltype = ColumnType.base(column.columnType())
        dtime_form = '%Y-%m-%d %H:%M:%S.%f'
        
        if value is None:
            return None
        elif coltype == ColumnType.Date:
            for form in ('%Y-%m-%d', dtime_form):
                try:
                    return datetime.datetime.strptime(nativestring(value), form).date()
                except StandardError:
                    continue
            
            logger.error('Unable to convert date: %s', value)
            return datetime.date.today()
        
        elif coltype == ColumnType.Time:
            for form in ('%H:%M:%S', dtime_form):
                try:
                    return datetime.datetime.strptime(nativestring(value), form).time()
                except StandardError:
                    continue
            
            logger.error('Unable to convert time: %s', value)
            return datetime.datetime.now().time()
        
        elif coltype in (ColumnType.Datetime, ColumnType.DatetimeWithTimezone):
            try:
                return datetime.datetime.strptime(nativestring(value), dtime_form)
            except StandardError:
                logger.error('Unable to convert datetime: %s', value)
                return datetime.datetime.now()
        
        elif coltype == ColumnType.Bool:
            if type(value) in (str, unicode):
                return value.lower() == 'true'
            
            try:
                return bool(value)
            except:
                return None
            
        elif coltype in (ColumnType.Integer, ColumnType.BigInt):
            try:
                return int(value)
            except:
                return None
        
        else:
            return super(SQLiteColumnEngine, self).unwrap(column, value)
    
    def wrap(self, column, value):
        """
        Wraps the value for this column.
        
        :param      column | <orb.Column>
                    value  | <variant>
        
        :return     <variant>
        """
        if type(value) == bool:
            return int(value)
        elif type(value) in (int, float):
            return long(value)
        elif type(value) == datetime.datetime:
            return value.strftime('%Y-%m-%d %H:%M:%S.%f')
        elif type(value) == datetime.date:
            return value.strftime('%Y-%m-%d')
        elif type(value) == datetime.time:
            return value.strftime('%H:%M:%S.%f')
        else:
            return super(SQLiteColumnEngine, self).wrap(column, value)

#----------------------------------------------------------------------

class SQLiteSchemaEngine(SqlSchemaEngine):
    def alterCommand(self, schema, columns=None):
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
            cols.append(engine.addCommand(col))
        
        # generate options
        output = []
        cmd = self.command('alter')
        for column in cols:
            opts = {}
            opts['table'] = self.wrapString(schema.tableName())
            opts['columns'] = column
            output.append(cmd.format(**opts))
        
        return ';\n'.join(output), {}
        
    
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
        if setup is None:
            setup = {}
        
        if len(records) == 1:
            return super(SQLiteSchemaEngine, self).insertCommand(schema,
                                                                 records,
                                                                 columns,
                                                                 autoincrement,
                                                                 first,
                                                                 setup)
        
        # generate the SQLite specific SQL for mutli-insertion
        commands = []
        data = {}
        if schema.inherits():
            acmd, adata = self.insertCommand(schema.ancestor(),
                                             records,
                                             autoincrement=autoincrement,
                                             first=False,
                                             setup=setup)
            
            commands.append(acmd)
            data.update(adata)
        
        # pre-generate the record ids
        elif autoincrement:
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
        
        columns = schema.columns(recurse=False,
                                 include=columns,
                                 includeProxies=False,
                                 includeJoined=False,
                                 includeAggregates=False)
        
        fields = map(lambda x: self.wrapString(x.fieldName()), columns)
        if schema.inherits():
            fields.append(self.wrapString('__inherits__'))
        
        cmd = 'INSERT INTO {0}'.format(self.wrapString(schema.tableName()))
        cmd += '('+','.join(fields)+')'
        
        db = self.backend().database()
        for r, record in enumerate(records):
            if Table.recordcheck(record):
                record = record.recordValues(autoInflate=False)
            
            line = []
            for column in columns:
                key = self.genkey(column.fieldName())
                field = self.wrapString(column.fieldName())
                if not r:
                    colcmd = '%({0})s AS {1}'
                else:
                    colcmd = '%({0})s'
                
                engine = column.engine(db)    
                data[key] = engine.wrap(column, record[column.name()])
                line.append(colcmd.format(key, field))
            
            if schema.inherits():
                key = self.genkey('__inherits__')
                if not r:
                    colcmd = '%({0})s AS `__inherits__`'
                else:
                    colcmd = '%({0})s'
                
                data[key] = record['_id']
                line.append(colcmd.format(key))
            
            if not r:
                cmd += '\n\tSELECT ' + ','.join(line)
            else:
                cmd += '\n\tUNION SELECT ' + ','.join(line)
        
        cmd += ';'
        commands.append(cmd)
        
        return '\n'.join(commands), data

#------------------------------------------------------------------------------

class SQLite(SqlBase):
    """ 
    Creates a SQLite backend connection type for handling database
    connections to SQLite databases.
    """
    
    def __init__( self, database ):
        super(SQLite, self).__init__(database,
                                     stringWrapper='`',
                                     schemaCommands=DEFAULT_SCHEMA_CMDS,
                                     columnCommands=DEFAULT_COLUMN_CMDS,
                                     typeMap=DEFAULT_TYPE_MAP,
                                     schemaEngine=SQLiteSchemaEngine,
                                     columnEngine=SQLiteColumnEngine)
        
        # update the integer column engine
        cmd = 'INTEGER PRIMARY KEY ASC'
        int_engine = self.columnEngine(ColumnType.Integer)
        int_engine.setCommand('create_primary', '{column} ' + cmd)
        int_engine.setCommand('add_primary', 'ADD COLUMN {column} ' + cmd)
        
        self.setInsertBatchSize(75)
    
    #----------------------------------------------------------------------
    #                       PROTECTED METHODS
    #----------------------------------------------------------------------
    
    def cleanup(self):
        """
        Performs a VACUUM operation on the database to clear out any
        unused memory or space.
        """
        self.execute('VACUUM')
    
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
        
        :return     [{<str> key: <variant>, ..}, ..]
        """
        rowcount = -1
        if data is None:
            data = []
        
        # when in debug mode, simply log the command to the logger
        elif self.database().commandsBlocked():
            logger.info(command)
            return [], rowcount
        
        # create a new cursor for this transaction
        db = self.backendDb()
        if db is None:
            raise errors.ConnectionLostError()
        
        cursor = db.cursor()
        
        logger.debug(command)
        
        # determine if we're inserting multiple statements at once
        commands = filter(lambda x: True if x else False, command.split(';'))
        if len(commands) > 1:
            db.isolation_level = 'IMMEDIATE'
            commands.insert(0, 'BEGIN TRANSACTION')
        else:
            db.isolation_level = None
        
        for cmd in commands:
            if not cmd:
                continue
            
            cmd += ';'
            
            # map the dictionary options to the param based for sqlite
            args = []
            for grp, key in FORMAT_EXPR.findall(cmd):
                value = ValueMapper.mappedValue(data[key])
                
                if type(value) in (list, tuple, set):
                    def _gen_subvalue(val):
                        out  = []
                        repl = []
                        
                        for sub_value in val:
                            if type(sub_value) in (list, tuple):
                                cmd, vals = _gen_subvalue(sub_value)
                                repl.append(cmd)
                                out += vals
                            else:
                                repl.append('?')
                                out.append(sub_value)
                        
                        return '(' + ','.join(repl) + ')', out
                    
                    repl, values = _gen_subvalue(value)
                    cmd = cmd.replace(grp, repl)
                    args += values
                else:
                    cmd = cmd.replace(grp, '?')
                    args.append(value)
            
            args = tuple(args)
            try:
                cursor.execute(cmd, args)
                rowcount = cursor.rowcount
            
            # check for interruption errors
            except sqlite.OperationalError, err:
                if nativestring(err) == 'interrupted':
                    raise errors.Interruption()
                else:
                    raise errors.DatabaseQueryError(command, data, err)
            
            except Exception, err:
                raise errors.DatabaseQueryError(command, data, err)
        
        if returning:
            results = map(mapper, cursor.fetchall())
        else:
            results = []
        
        if autoCommit:
            self.commit()
        
        db.isolation_level = None
        if autoClose:
            cursor.close()
        
        return results, rowcount
        
    def _open(self, db):
        """
        Opens a new database connection to the datbase defined
        by the inputed database.
        
        :param      db | <orb.Database>
        
        :return     <bool> success
        """
        # make sure we have a sqlite module
        if not sqlite:
            raise errors.MissingBackend('Could not import sqlite3')
        
        # create the name of the database
        dbname = os.path.normpath(nativestring(db.databaseName()))
        
        # create the python connection
        try:
            sqlitedb = sqlite.connect(dbname)
            sqlitedb.create_function('REGEXP', 2, matches)
            sqlitedb.row_factory = dict_factory
            sqlitedb.text_factory = str
            return sqlitedb
        
        except sqlite.Error:
            raise errors.ConnectionError('Error connecting to sqlite', db)
    
    def _interrupt(self, threadId, backendDb):
        """
        Interrupts the given backend database connection from a separate thread.
        
        :param      threadid  | <int>
                    backendDb | <variant> | backend specific database.
        """
        backendDb.interrupt()
    
    #----------------------------------------------------------------------
    #                       PUBLIC METHODS
    #----------------------------------------------------------------------
    
    def existingColumns(self, schema, namespace=None, mapper=None):
        """
        Looks up the existing columns from the database based on the
        inputed schema and namespace information.
        
        :param      schema      | <orb.TableSchema>
                    namespace   | <str> || None
        
        :return     [<str>, ..]
        """
        if mapper is None:
            mapper = lambda x: x['name']
        
        return super(SQLite, self).existingColumns(schema, namespace, mapper)

    
# register the sqlite backend
if sqlite:
    Connection.register('SQLite', SQLite)