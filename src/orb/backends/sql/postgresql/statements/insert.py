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

class INSERT(PSQL):
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
        
        scope.setdefault('__sql__', PSQL)
        
        return super(INSERT, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('INSERT', INSERT(PSQL.load(__name__)))

