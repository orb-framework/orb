"""
The search library of ORB provides basic search functionality to all ORB models.  It will also provide a
base class for more advanced searching capabilities such as AWS or Elasticsearch to be applied to particular models
during development.
"""

import logging
from abc import ABCMeta, abstractmethod

log = logging.getLogger(__name__)


class SearchEngine(object):
    """ Defines the base class """
    __metaclass__ = ABCMeta
    __factory__ = None

    @abstractmethod
    def search(self, model, terms, **context):
        """
        Implements the search logic for this engine.  Given the options
        provided within the context, this method should return either
        an `<orb.Collection>` of results (if `returning=records`) of the
        search, or the raw data (if `returning=data`) from the search
        engine.

        :param model: subclass of <orb.Model>
        :param terms: <str>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        raise NotImplementedError

    @classmethod
    def factory(cls, name, **kw):
        """
        Creates a new search engine by the factory name.

        :param name: <str>
        :param kw: <dict> engine options

        :return: <orb.SearchEngine>
        """
        for sub_cls in cls.__subclasses__():
            if sub_cls.__factory__ == name:
                return sub_cls(**kw)
        else:
            raise RuntimeError('Factory not found')
