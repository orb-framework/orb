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

class ENABLE_INTERNALS(PSQL):
    def render(self, enabled, schema=None, **scope):
        scope['enabled'] = enabled
        scope['schema'] = database
        
        return super(ENABLE_INTERNALS, self).render(**scope)

# register the statement to the addon
PSQL.registerAddon('ENABLE INTERNALS', ENABLE_INTERNALS(PSQL.load(__name__)))

