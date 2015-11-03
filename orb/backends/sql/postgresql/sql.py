"""
Defines the base PSQL class used for all Postgres based statements.
"""

import orb
from ..abstractsql import SQL
from .statements import __plugins__


class PSQL(SQL):
    @classmethod
    def createDatastore(cls):
        """
        Returns the PSQL data store class.
        
        :return     subclass of <orb.DataStore>
        """
        store_type = orb.DataStore.byName('Postgres', orb.DataStore)
        return store_type()

# load the SQL plugins for PostgreSQL
PSQL.loadStatements(__plugins__)

# ----------------------------------------------------------------------

# define custom column types for PostgreSQL
PSQL.registerAddon('Type::Bool', u'BOOLEAN')
PSQL.registerAddon('Type::BigInt', u'BIGINT')
PSQL.registerAddon('Type::ByteArray', u'BYTEA')
PSQL.registerAddon('Type::Color', u'CHARACTER VARYING')
PSQL.registerAddon('Type::Datetime', u'TIMESTAMP WITHOUT TIME ZONE')
PSQL.registerAddon('Type::DatetimeWithTimezone', u'TIMESTAMP WITHOUT TIME ZONE')
PSQL.registerAddon('Type::Decimal', u'DECIMAL')
PSQL.registerAddon('Type::Dict', u'BYTEA')
PSQL.registerAddon('Type::Directory', u'CHARACTER VARYING')
PSQL.registerAddon('Type::Double', u'DOUBLE PRECISION')
PSQL.registerAddon('Type::Email', u'CHARACTER VARYING')
PSQL.registerAddon('Type::Enum', u'INTEGER')
PSQL.registerAddon('Type::Filepath', u'CHARACTER VARYING')
PSQL.registerAddon('Type::ForeignKey', u'BIGINT')
PSQL.registerAddon('Type::Image', u'BYTEA')
PSQL.registerAddon('Type::Integer', u'INTEGER')
PSQL.registerAddon('Type::Interval', u'TIMEDELTA')
PSQL.registerAddon('Type::Password', u'CHARACTER VARYING')
PSQL.registerAddon('Type::Pickle', u'BYTEA')
PSQL.registerAddon('Type::String', u'CHARACTER VARYING')
PSQL.registerAddon('Type::Url', u'CHARACTER VARYING')

# register the PSQLStatement addon interface to the base SQLStatement class
SQL.registerAddon('Postgres', PSQL)

# register the statements addons for PSQL
from .statements import __plugins__

PSQL.registerAddonModule(__plugins__)

