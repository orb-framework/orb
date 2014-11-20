"""
Defines the base SQL class used for rendering SQL statements
out.
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

import logging
import mako
import mako.template
import os
import orb
import sys

from orb import errors
from projex.addon import AddonManager

log = logging.getLogger(__name__)

class SQL(AddonManager):
    def __init__(self, sql, baseSQL=None):
        super(SQL, self).__init__()
        
        # define custom properties
        self._template = mako.template.Template(sql)
        self._sql = sql
        self._baseSQL = baseSQL or SQL

    def __call__(self, *args, **options):
        """
        Executes this statement with the inputed keywords to generate
        the context SQL statement.
        
        :param      **options | <keywords>
        
        :sa         render
        
        :return     <str> sql, <dict> data
        """
        return self.render(*args, **options)

    def baseSQL(self):
        """
        Returns the base SQL type to use for this instance.
        
        :return     subclass of <SQL>
        """
        return self._baseSQL

    def render(self, **scope):
        """
        Executes this statement with the inputed keywords to generate
        the context SQL statement.  Any keywords provided to the render
        method will be used as scope variables within the mako template for
        this SQL class.
        
        :param      **scope | <keywords>
        
        On top of any provided scope variables, the global
        options will be:
        
        :globals    __data__      | <dict> | input/output dictionary for defining \
                                             variables to be provided to the execute \
                                             method
                    __db__        | <orb.Database> | calling database
                    __manager__   | <orb.Manager> | calling manager
                    __namespace__ | <str>
                    __sql__       | <orb.AbstractSQL> | SQL statement manager
                    orb           | <module>
        
        :return     <str> sql, <dict> data
        """
        # define common properties
        scope.setdefault('__data__', {})
        scope.setdefault('__manager__', orb.system)
        scope.setdefault('__db__', scope['__manager__'].database())
        scope.setdefault('__sql__', self.baseSQL())
        scope.setdefault('__namespace__', '')
        
        # module imports
        scope.setdefault('orb', orb)
        
        scope['__data__'].setdefault('output', scope.get('output', {}))
        
        sql = self._template.render(**scope)
        return sql.strip(), scope['__data__']['output']

    def setSQL(self, sql):
        """
        Sets the SQL mako statement for this instance.  This will generate
        a new mako Template that will be used when executing this command
        during generation.
        
        :param      sql | <str>
        """
        self._sql = sql
        self._template = mako.template.Template(sql)

    def sql(self):
        """
        Returns the template for this statement.
        
        :return     <str>
        """
        return self._sql

    @classmethod
    def createDatastore(cls):
        """
        Creates a new datastore instance for this sql class.
        
        :return     <orb.DataStore>
        """
        return orb.DataStore()

    @classmethod
    def datastore(cls):
        """
        Returns the base data store class for this SQL definition.
        
        :return     subclass of <orb.DataStore>
        """
        key = '_{0}__datastore'.format(cls.__name__)
        try:
            return getattr(cls, key)
        except AttributeError:
            store = cls.createDatastore()
            setattr(cls, key, store)
            return store

    @classmethod
    def loadStatements(cls, module):
        """
        Loads the mako definitions for the inputed name.  This is the inputed
        module that will be attepmting to access the file.  When running
        with mako file support, this will read and load the mako file, when 
        built it will load a _mako.py module that defines the TEMPLATE variable
        as a string.
        
        :param      name | <str>
        
        :return     <str>
        """
        # load from the built table of contents
        if hasattr(module, '__toc__') and module.__toc__:
            mako_mods = module.__toc__
            for mako_mod in mako_mods:
                try:
                    __import__(mako_mod)
                    templ = sys.modules[mako_mod].TEMPLATE
                except StandardError:
                    log.error('Failed to load mako file: {0}'.format(mako_mod))
                    continue
                else:
                    name = mako_mod.split('.')[-1].replace('_mako', '').upper()
                    typ = globals().get(name, SQL)
                    cls.registerAddon(name, typ(templ, cls))
        
        # load from the directory
        else:
            base = os.path.dirname(module.__file__)
            files = os.listdir(os.path.dirname(module.__file__))
            for filename in files:
                if not filename.endswith('.mako'):
                    continue
                
                with open(os.path.join(base, filename), 'r') as f:
                    templ = f.read()
                
                name = filename.split('.')[0].upper()
                typ = globals().get(name, SQL)
                cls.registerAddon(name, typ(templ, cls))

#----------------------------------------------------------------------
#                       DEFINE BASE SQL COMMANDS
#----------------------------------------------------------------------

# A
#----------------------------------------------------------------------

class ADD_COLUMN(SQL):
    def render(self, column, **scope):
        """
        Generates the ADD COLUMN sql for an <orb.Column> in Postgres.
        
        :param      column      | <orb.Column>
                    **scope   | <keywords>
        
        :return     <str>
        """
        scope['column'] = column
        
        return super(ADD_COLUMN, self).render(**scope)

class ALTER_TABLE(SQL):
    def render(self, schema, added=None, removed=None, **scope):
        """
        Generates the ALTER TABLE sql for an <orb.Table>.
        
        :param      schema  | <orb.TableSchema>
                    added   | [<orb.Column>, ..] || None
                    removed | [<orb.Column>, ..] || None
                    **scope | <dict>
        
        :return     <str>
        """
        scope['schema'] = schema
        scope['added'] = added if added is not None else []
        scope['removed'] = removed if removed is not None else []
        
        return super(ALTER_TABLE, self).render(**scope)

# C
#----------------------------------------------------------------------

class CREATE_TABLE(SQL):
    def render(self, table, **scope):
        """
        Generates the CREATE TABLE sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        
        return super(CREATE_TABLE, self).render(**scope)

# D
#----------------------------------------------------------------------

class DELETE(SQL):
    def render(self, table, where, **scope):
        """
        Generates the DELETE sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    where   | <orb.Query>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        scope['where'] = where
        
        return super(DELETE, self).render(**scope)

# E
#----------------------------------------------------------------------

class ENABLE_INTERNALS(SQL):
    def render(self, enabled, schema=None, **scope):
        scope['enabled'] = enabled
        scope['schema'] = database
        
        return super(ENABLE_INTERNALS, self).render(**scope)

# I
#----------------------------------------------------------------------

class INSERT(SQL):
    def render(self, schema, records, columns=None, **scope):
        """
        Generates the INSERT sql for an <orb.Table>.
        
        :param      schema  | <orb.Table> || <orb.TableSchema>
                    records | [<orb.Table>, ..]
                    columns | [<str>, ..]
                    **scope | <dict>
        
        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()
        
        if columns is None:
            columns = schema.columns(includeJoined=False,
                                     includeAggregates=False,
                                     includeProxies=False)
        else:
            columns = map(schema.column, columns)
        
        scope['schema'] = schema
        scope['records'] = records
        scope['columns'] = columns
        
        return super(INSERT, self).render(**scope)

class INSERTED_KEYS(SQL):
    def render(self, schema, count=1, **scope):
        """
        Generates the INSERTED KEYS sql for an <orb.Table> or <orb.TableSchema>.
        
        :param      schema  | <orb.Table> || <orb.TableSchema>
                    count   | <int>
                    **scope | <dict>
        
        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()
        
        scope['schema'] = schema
        scope['count'] = count
        
        return super(INSERTED_KEYS, self).render(**scope)

# S
#----------------------------------------------------------------------

class SELECT(SQL):
    def render(self, table, **scope):
        """
        Generates the TABLE EXISTS sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        scope['lookup'] = scope.get('lookup', orb.LookupOptions(**scope))
        scope['options'] = scope.get('options', orb.DatabaseOptions(**scope))
        
        return super(SELECT, self).render(**scope)

class SELECT_AGGREGATE(SQL):
    def render(self, column, **scope):
        """
        Generates the SELECT AGGREGATE sql for an <orb.Table>.
        
        :param      column   | <orb.Column>
                    **scope  | <dict>
        
        :return     <str>
        """
        scope['column'] = column
        
        return super(SELECT_AGGREGATE, self).render(**scope)

class SELECT_COUNT(SQL):
    def render(self, table, **scope):
        """
        Generates the SELECT COUNT sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    lookup  | <orb.LookupOptions>
                    options | <orb.DatabaseOptions>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        scope['lookup'] = scope.get('lookup', orb.LookupOptions(**scope))
        scope['options'] = scope.get('options', orb.DatabaseOptions(**scope))
        
        return super(SELECT_COUNT, self).render(**scope)

class SELECT_EXPAND(SQL):
    def render(self, **scope):
        return super(SELECT_EXPAND, self).render(**scope)

class SELECT_JOINER(SQL):
    def render(self, column, **scope):
        """
        Generates the SELECT JOINER sql for an <orb.Table>.
        
        :param      column   | <orb.Column>
                    **scope  | <dict>
        
        :return     <str>
        """
        scope['column'] = column
        
        return super(SELECT_JOINER, self).render(**scope)

# T
#----------------------------------------------------------------------

class TABLE_COLUMNS(SQL):
    def render(self, schema, **scope):
        """
        Generates the TABLE COLUMNS sql for an <orb.Table>.
        
        :param      schema  | <orb.Table> || <orb.TableSchema>
                    **scope | <dict>
        
        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()
        
        scope['schema'] = schema
        
        return super(TABLE_COLUMNS, self).render(**scope)

class TABLE_EXISTS(SQL):
    def render(self, schema, **scope):
        """
        Generates the TABLE EXISTS sql for an <orb.Table>.
        
        :param      schema  | <orb.TableSchema> || <orb.Table>
                    **scope | <dict>
        
        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()
        
        scope['schema'] = schema
        
        return super(TABLE_EXISTS, self).render(**scope)

# U
#----------------------------------------------------------------------

class UPDATE(SQL):
    def render(self, schema, changes, **scope):
        """
        Generates the UPDATE sql for an <orb.Table>.
        
        :param      schema  | <orb.Table> || <orb.TableSchema>
                    changes | [(<orb.Table>, [<orb.Column>, ..]) ..]
                    **scope | <dict>
        
        :return     <str>
        """
        if orb.Table.typecheck(schema):
            schema = schema.schema()
        
        scope['schema'] = schema
        scope['changes'] = changes
        
        return super(UPDATE, self).render(**scope)

# W
#----------------------------------------------------------------------

class WHERE(SQL):
    def render(self, where, baseSchema=None, **scope):
        """
        Generates the WHERE sql for an <orb.Table>.
        
        :param      where   | <orb.Query> || <orb.QueryCompound>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['baseSchema'] = baseSchema
        scope['where'] = where
        
        return super(WHERE, self).render(**scope)

#----------------------------------------------------------------------

# define base column types
SQL.registerAddon('Type::BigInt',                   u'BIGINT')
SQL.registerAddon('Type::Bool',                     u'BOOL')
SQL.registerAddon('Type::ByteArray',                u'VARBINARY')
SQL.registerAddon('Type::Color',                    u'VARCHAR')
SQL.registerAddon('Type::Date',                     u'DATE')
SQL.registerAddon('Type::Datetime',                 u'DATETIME')
SQL.registerAddon('Type::DatetimeWithTimezone',     u'TIMESTAMP')
SQL.registerAddon('Type::Decimal',                  u'DECIMAL UNSIGNED')
SQL.registerAddon('Type::Directory',                u'VARCHAR')
SQL.registerAddon('Type::Dict',                     u'BLOB')
SQL.registerAddon('Type::Double',                   u'DOUBLE UNSIGNED')
SQL.registerAddon('Type::Email',                    u'VARCHAR')
SQL.registerAddon('Type::Enum',                     u'INT UNSIGNED')
SQL.registerAddon('Type::Filepath',                 u'VARCHAR')
SQL.registerAddon('Type::ForeignKey',               u'BIGINT UNSIGNED')
SQL.registerAddon('Type::Html',                     u'TEXT')
SQL.registerAddon('Type::Image',                    u'BLOB')
SQL.registerAddon('Type::Integer',                  u'INT UNSIGNED')
SQL.registerAddon('Type::Password',                 u'VARCHAR')
SQL.registerAddon('Type::Pickle',                   u'BLOB')
SQL.registerAddon('Type::Query',                    u'TEXT')
SQL.registerAddon('Type::String',                   u'VARCHAR')
SQL.registerAddon('Type::Text',                     u'TEXT')
SQL.registerAddon('Type::Time',                     u'TIME')
SQL.registerAddon('Type::Url',                      u'VARCHAR')
SQL.registerAddon('Type::Xml',                      u'TEXT')
SQL.registerAddon('Type::Yaml',                     u'TEXT')

# define the default lengths
SQL.registerAddon('Length::Color',                  25)
SQL.registerAddon('Length::String',                 256)
SQL.registerAddon('Length::Email',                  256)
SQL.registerAddon('Length::Password',               256)
SQL.registerAddon('Length::Url',                    500)
SQL.registerAddon('Length::Filepath',               500)
SQL.registerAddon('Length::Directory',              500)

# define the base flags
SQL.registerAddon('Flag::Unique',                   u'UNIQUE')
SQL.registerAddon('Flag::Required',                 u'NOT NULL')
SQL.registerAddon('Flag::AutoIncrement',            u'AUTO_INCREMENT')

# define the base operators
SQL.registerAddon('Op::Is',                              u'=')
SQL.registerAddon('Op::IsNot',                           u'!=')
SQL.registerAddon('Op::LessThan',                        u'<')
SQL.registerAddon('Op::Before',                          u'<')
SQL.registerAddon('Op::LessThanOrEqual',                 u'<=')
SQL.registerAddon('Op::GreaterThanOrEqual',              u'>=')
SQL.registerAddon('Op::GreaterThan',                     u'>')
SQL.registerAddon('Op::After',                           u'>')
SQL.registerAddon('Op::Matches',                         u'~*')
SQL.registerAddon('Op::Matches::CaseSensitive',          u'~')
SQL.registerAddon('Op::DoesNotMatch',                    u'!~*')
SQL.registerAddon('Op::DoesNotMatch::CaseSensitive',     u'!~*')
SQL.registerAddon('Op::Contains',                        u'ILIKE')
SQL.registerAddon('Op::Contains::CaseSensitive',         u'LIKE')
SQL.registerAddon('Op::Startswith',                      u'ILIKE')
SQL.registerAddon('Op::Startswith::CaseSensitive',       u'LIKE')
SQL.registerAddon('Op::Endswith',                        u'ILIKE')
SQL.registerAddon('Op::Endswith::CaseSensitive',         u'LIKE')
SQL.registerAddon('Op::DoesNotContain',                  u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotContain::CaseSensitive',   u'NOT LIKE')
SQL.registerAddon('Op::DoesNotStartwith',                u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotStartwith::CaseSensitive', u'NOT LIKE')
SQL.registerAddon('Op::DoesNotEndwith',                  u'NOT ILIKE')
SQL.registerAddon('Op::DoesNotEndwith::CaseSensitive',   u'NOT LIKE')
SQL.registerAddon('Op::IsIn',                            u'IN')
SQL.registerAddon('Op::IsNotIn',                         u'NOT IN')

# define the base functions
SQL.registerAddon('Func::Lower',                    u'lower({0})')
SQL.registerAddon('Func::Upper',                    u'upper({0})')
SQL.registerAddon('Func::Abs',                      u'abs({0})')
SQL.registerAddon('Func::AsString',                 u'{0}::varchar')

# define the base math operators
SQL.registerAddon('Math::Add',                      u'+')
SQL.registerAddon('Math::Subtract',                 u'-')
SQL.registerAddon('Math::Multiply',                 u'*')
SQL.registerAddon('Math::Divide',                   u'/')
SQL.registerAddon('Math::And',                      u'&')
SQL.registerAddon('Math::Or',                       u'|')

SQL.registerAddon('Math::Add::String',              u'||')
SQL.registerAddon('Math::Add::Text',                u'||')

