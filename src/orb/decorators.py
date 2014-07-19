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

from projex.decorators  import wraps
from orb.caching        import RecordCache
from orb.transaction    import Transaction

# C
#------------------------------------------------------------------------------

def cachedmethod( *tables ):
    """
    Defines a decorator method to wrap a method with a caching mechanism.
    
    :param      *tables | [<orb.Table>, ..]
    """
    def decorated(func):
        cache = RecordCache(*tables)
        
        @wraps(func)
        def wrapped(*args, **kwds):
            cache.begin()
            results = func(*args, **kwds)
            cache.end()
            
            return results
        
        # expose access to the record cache
        wrapped.__dict__['cache'] = cache
        
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
            # create a transaction
            transaction = Transaction()
            
            # run the function within a transaction
            transaction.begin()
            results = func(*args, **kwds)
            transaction.end()
            
            return results
        return wrapped
    return decorated