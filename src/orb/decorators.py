""" Defines decorator methods for the ORB library. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software, LLC'
__license__         = 'LGPL'

__maintainer__      = 'Projex Software, LLC'
__email__           = 'team@projexsoftware.com'

import time

from projex.lazymodule import LazyModule
from projex.decorators import wraps

orb = LazyModule('orb')


# C
#------------------------------------------------------------------------------

def cachedmethod(*tables):
    """
    Defines a decorator method to wrap a method with a caching mechanism.
    
    :param      *tables | [<orb.Table>, ..]
    """
    def decorated(func):
        @wraps(func)
        def wrapped(*args, **kwds):
            with cache:
                results = func(*args, **kwds)
            return results
        
        # expose access to the record cache
        wrapped.__dict__['cache'] = orb.RecordCache(*tables)
        
        return wrapped
    return decorated

# T
#------------------------------------------------------------------------------

def transactedmethod():
    """
    Defines a decorator method to wrap a method in a transaction mechanism.
    """
    def decorated(func):
        @wraps(func)
        def wrapped(*args, **kwds):
            # run the function within a transaction
            with orb.Transaction() as transaction:
                results = func(*args, **kwds)
            return results
        return wrapped
    return decorated

