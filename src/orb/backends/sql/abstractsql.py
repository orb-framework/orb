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
    def __init__(self, sql):
        super(SQL, self).__init__()
        
        # define custom properties
        self._template = mako.template.Template(sql)
        self._sql = sql

    def __call__(self, *args, **options):
        """
        Executes this statement with the inputed keywords to generate
        the context SQL statement.
        
        :param      **options | <keywords>
        
        :sa         render
        
        :return     <str> sql, <dict> data
        """
        return self.render(*args, **options)

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
        scope.setdefault('__sql__', type(self))
        scope.setdefault('__namespace__', '')
        
        # module imports
        scope.setdefault('orb', orb)
        
        scope['__data__'].setdefault('output', {})
        
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

    @staticmethod
    def load(name):
        """
        Loads the mako definition for the inputed name.  This is the inputed
        module that will be attepmting to access the file.  When running
        with mako file support, this will read and load the mako file, when 
        built it will load a _mako.py module that defines the TEMPLATE variable
        as a string.
        
        :param      name | <str>
        
        :return     <str>
        """
        mako_mod = name + '_mako'
        try:
            __import__(mako_mod)
            return sys.modules[mako_mod].TEMPLATE
        except StandardError:
            try:
                mod = sys.modules[name]
                path = os.path.dirname(mod.__file__)
                filename = os.path.join(path, name.split('.')[-1] + '.mako')
                with open(filename, 'r') as f:
                    return f.read()
            except StandardError:
                return ''

# define base columnn types
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

