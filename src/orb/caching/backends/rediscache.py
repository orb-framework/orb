import projex.rest

from projex.lazymodule import lazy_import
from orb.caching.datacache import DataCache

redis = lazy_import('redis')
orb = lazy_import('orb')


class RedisCache(DataCache):
    """ Base caching object for tracking data caches """
    def __init__(self, host='localhost', port=6379, timeout=0):
        super(RedisCache, self).__init__(timeout)

        # define custom properties
        self._client = redis.StrictRedis(host=host, port=port)

    def expire(self, key):
        if key:
            self._client.delete(key)
        else:
            self._client.flushall()

    def isCached(self, key):
        """
        Returns whether or not the inputed key is cached.

        :param      key | <hashable>

        :return     <bool>
        """
        return self._client.exists(key)

    def isExpired(self, key):
        """
        Returns whether or not the current cache is expired.

        :return     <bool>
        """
        return self._client.get(key) is None

    def setValue(self, key, value):
        """
        Caches the inputed key and value to this instance.

        :param      key     | <hashable>
                    value   | <variant>
        """
        self._client.set(key, projex.rest.jsonify(value))
        self._client.expire(key, int(self.timeout()))

    def value(self, key, default=None):
        """
        Returns the value for the cached key for this instance.

        :return     <variant>
        """
        value = self._client.get(key)
        if value is not None:
            return projex.rest.unjsonify(value)
        else:
            return default

# create a basic cache object
DataCache.registerAddon('Redis', RedisCache)