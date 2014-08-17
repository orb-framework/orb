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

class SELECT_AGGREGATE(PSQL):
    def render(self, column, **scope):
        """
        Generates the SELECT AGGREGATE sql for an <orb.Table>.
        
        :param      column   | <orb.Column>
                    **scope  | <dict>
        
        :return     <str>
        """
        scope['column'] = column
        scope.setdefault('__sql__', PSQL)
        
        return super(SELECT_AGGREGATE, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('SELECT AGGREGATE', SELECT_AGGREGATE(PSQL.load(__name__)))

