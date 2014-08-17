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

class INSERTED_KEYS(PSQL):
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
        
        scope.setdefault('__sql__', PSQL)
        
        return super(INSERTED_KEYS, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('INSERTED KEYS', INSERTED_KEYS(PSQL.load(__name__)))

