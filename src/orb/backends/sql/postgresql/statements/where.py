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

# define the SQL
class WHERE(PSQL):
    def render(self, where, baseSchema=None, **scope):
        """
        Generates the WHERE sql for an <orb.Table>.
        
        :param      where   | <orb.Query> || <orb.QueryCompound>
                    **scope | <dict>
        
        :return     <str>
        """
        scope['baseSchema'] = baseSchema
        scope['where'] = where
        scope.setdefault('__sql__', PSQL)
        
        return super(WHERE, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('WHERE', WHERE(PSQL.load(__name__)))

