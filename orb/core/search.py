from projex.lazymodule import lazy_import
from projex.addon import AddonManager

orb = lazy_import('orb')


class SearchEngine(AddonManager):
    def __init__(self):
        pass


class SearchResultCollection(object):
    def __json__(self):
        data = {
            'results': [r.__json__() for r in self.results()]
        }
        return data

    def __init__(self, terms, **context):
        self.__context = orb.Context(**context)
        self.__terms = terms

    def results(self):
        return []


class SearchResult(object):
    def __json__(self):
        data = {
            'record': self.__record.__json__(),
            'rank': self.__rank
        }
        return data

    def __init__(self, record, rank):
        self.__record = record, rank
        self.__rank = rank

    def rank(self):
        """
        Returns the rank for this search result.

        :return:    <int>
        """
        return self.__rank

    def record(self):
        """
        Returns the record that was found for this search result.

        :return:    <orb.Model>
        """
        return self.__record