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

class ALTER_TABLE(PSQL):
    def render(self, table, added=None, removed=None, **scope):
        """
        Generates the ALTER TABLE sql for an <orb.Table>.
        
        :param      table   | <orb.Table>
                    added   | [<orb.Column>, ..] || None
                    removed | [<orb.Column>, ..] || None
                    **scope | <dict>
        
        :return     <str>
        """
        scope['table'] = table
        scope['added'] = added if added is not None else []
        scope['removed'] = removed if removed is not None else []
        
        scope.setdefault('__sql__', PSQL)
        
        return super(ALTER_TABLE, self).render(**scope)


# register the statement to the addon
PSQL.registerAddon('ALTER TABLE', ALTER_TABLE(PSQL.load(__name__)))

