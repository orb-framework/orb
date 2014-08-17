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

from ..sql import PSQL

class CREATE_TABLE(PSQL):
    def render(self, table, **scope):
        """
        Generates the CREATE TABLE sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        scope.setdefault('__sql__', PSQL)
        
        return super(CREATE_TABLE, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('CREATE TABLE', CREATE_TABLE(PSQL.load(__name__)))

