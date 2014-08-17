#!/usr/bin/python

""" Defines the backend connection class for PostgreSQL databases. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

import orb
from ..sql import PSQL

class TABLE_COLUMNS(PSQL):
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
        scope.setdefault('__sql__', PSQL)
        
        return super(TABLE_COLUMNS, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('TABLE COLUMNS', TABLE_COLUMNS(PSQL.load(__name__)))

