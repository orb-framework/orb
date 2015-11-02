import cPickle

from projex.lazymodule import lazy_import
from orb.caching.datacache import DataCache

redis = lazy_import('redis')
orb = lazy_import('orb')


# noinspection PyAbstractClass
class RedisCache(DataCache):
    """ Base caching object for tracking data caches """
    def __init__(self, host='localhost', port=6379, timeout=0):
        super(RedisCache, self).__init__(timeout)

        # define custom properties
        self._client = redis.StrictRedis(host=host, port=port)

    @staticmethod
    def __key(key):
        return 'ORB({0})'.format(key or '*')

    def expire(self, key=None):
        """
        Expires out all the ORB related keys from the redis cache.

        :param      key | <hashable>
        """
        if key:
            key = self.__key(key)
            self._client.delete(self.__key(key))
        else:
            keys = self._client.keys(self.__key(key))
            with self._client.pipeline() as pipe:
                pipe.multi()
                for key in keys:
                    pipe.delete(key)
                pipe.execute()

    def isCached(self, key):
        """
        Returns whether or not the inputted key is cached.

        :param      key | <hashable>

        :return     <bool>
        """
        return self._client.exists(self.__key(key))

    def setValue(self, key, value, timeout=None):
        """
        Caches the inputted key and value to this instance.

        :param      key     | <hashable>
                    value   | <variant>
        """
        key = self.__key(key)
        self._client.set(key, cPickle.dumps(value))
        self._client.expire(key, int(timeout or self.timeout()))

    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.

        :return     <variant>
        """
        key = self.__key(key)
        value = self._client.get(key)
        if value is not None:
            return cPickle.loads(value)
        else:
            return default

# create a basic cache object
DataCache.registerAddon('Redis', RedisCache)