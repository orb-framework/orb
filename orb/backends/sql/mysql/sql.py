"""
Defines the base MySQL class used for all MySQL based statements.
"""

import orb
from ..abstractsql import SQL
from .statements import __plugins__


class MySQL(SQL):
    @classmethod
    def createDatastore(cls):
        """
        Returns the MySQL data store class.
        
        :return     subclass of <orb.DataStore>
        """
        store_type = orb.DataStore.byName('MySQL', orb.DataStore)
        return store_type()

# load the SQL plugins for MySQL
MySQL.loadStatements(__plugins__)

# ----------------------------------------------------------------------

# define custom column types for MySQL
MySQL.registerAddon('Type::Bool', u'BOOLEAN')
MySQL.registerAddon('Type::BigInt', u'BIGINT')
MySQL.registerAddon('Type::ByteArray', u'BYTEA')
MySQL.registerAddon('Type::Color', u'CHARACTER VARYING')
MySQL.registerAddon('Type::Datetime', u'TIMESTAMP WITHOUT TIME ZONE')
MySQL.registerAddon('Type::DatetimeWithTimezone', u'TIMESTAMP WITHOUT TIME ZONE')
MySQL.registerAddon('Type::Decimal', u'DECIMAL')
MySQL.registerAddon('Type::Dict', u'BYTEA')
MySQL.registerAddon('Type::Directory', u'CHARACTER VARYING')
MySQL.registerAddon('Type::Double', u'DOUBLE PRECISION')
MySQL.registerAddon('Type::Email', u'CHARACTER VARYING')
MySQL.registerAddon('Type::Enum', u'INTEGER')
MySQL.registerAddon('Type::Filepath', u'CHARACTER VARYING')
MySQL.registerAddon('Type::ForeignKey', u'BIGINT')
MySQL.registerAddon('Type::Image', u'BYTEA')
MySQL.registerAddon('Type::Integer', u'INTEGER')
MySQL.registerAddon('Type::Interval', u'TIMEDELTA')
MySQL.registerAddon('Type::Password', u'CHARACTER VARYING')
MySQL.registerAddon('Type::Pickle', u'BYTEA')
MySQL.registerAddon('Type::String', u'CHARACTER VARYING')
MySQL.registerAddon('Type::Url', u'CHARACTER VARYING')

# register the MySQLStatement addon interface to the base SQLStatement class
SQL.registerAddon('MySQL', MySQL)

# register the statements addons for MySQL
from .statements import __plugins__

MySQL.registerAddonModule(__plugins__)

