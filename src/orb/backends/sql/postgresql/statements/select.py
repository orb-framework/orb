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
from orb import errors

from ..sql import PSQL

class SELECT(PSQL):
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
        scope.setdefault('__sql__', PSQL)
        
        return super(SELECT, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('SELECT', SELECT(PSQL.load(__name__)))

