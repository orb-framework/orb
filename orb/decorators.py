""" Defines decorator methods for the ORB library. """

from projex.lazymodule import lazy_import
from projex.decorators import wraps

orb = lazy_import('orb')


# C
# -----------------------------------------------------------------------------

def cachedmethod(*tables):
    """
    Defines a decorator method to wrap a method with a caching mechanism.
    
    :param      *tables | [<orb.Table>, ..]
    """
    def decorated(func):
        @wraps(func)
        def wrapped(*args, **kwds):
            with wrapped['cache']:
                results = func(*args, **kwds)
            return results
        
        # expose access to the record cache
        wrapped.__dict__['cache'] = orb.RecordCache(*tables)
        
        return wrapped
    return decorated

# L
# -----------------------------------------------------------------------------


def lookupmethod(cache=None, permits=None):
    def wrapped(method):
        method.permits = permits
        method.cache_timeout = cache

        def caller(source, *args, **options):
            data_cache = None
            if method.cache_timeout:
                try:
                    data_cache = source.tableCache()
                except AttributeError:
                    raise StandardError('Invalid type for a lookupmethod: {0}'.format(source))

                key = hash(args), hash(orb.LookupOptions(**options)), hash(orb.ContextOptions(**options))
                if data_cache:
                    try:
                        return cache[key]
                    except KeyError:
                        pass
            else:
                key = None

            options.setdefault('context', method.__name__)
            output = method(source, *args, **options)
            if data_cache:
                data_cache.setValue(key, output, timeout=method.cache_timeout)

            return output

        caller.__lookup__ = True

        return caller
    return wrapped

# T
# -----------------------------------------------------------------------------


def transactedmethod():
    """
    Defines a decorator method to wrap a method in a transaction mechanism.
    """
    def decorated(func):
        @wraps(func)
        def wrapped(*args, **kwds):
            # run the function within a transaction
            with orb.Transaction():
                results = func(*args, **kwds)
            return results
        return wrapped
    return decorated

