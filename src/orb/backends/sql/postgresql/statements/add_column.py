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

class ADD_COLUMN(PSQL):
    def render(self, column, **scope):
        """
        Generates the ADD COLUMN sql for an <orb.Column> in Postgres.
        
        :param      column      | <orb.Column>
                    **scope   | <keywords>
        
        :return     <str>
        """
        scope['column'] = column
        scope.setdefault('__sql__', PSQL)
        
        return super(ADD_COLUMN, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('ADD COLUMN', ADD_COLUMN(PSQL.load(__name__)))
