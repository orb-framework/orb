import projex.text

from projex.enum import enum
from projex.lazymodule import lazy_import

orb = lazy_import('orb')


class Collector(object):
    Flags = enum(
        'Unique',
        'Private',
        'ReadOnly',
        'Virtual',
        'Static',
        'AutoExpand'
    )

    def __json__(self):
        try:
            model = self.model()
        except orb.errors.ModelNotFound:
            model_name = None
        else:
            model_name = model.schema().name() if model else None

        output = {
            'name': self.__name,
            'model': model_name,
            'flags': {k: True for k in self.Flags.toSet(self.__flags)}
        }
        return output

    def __init__(self,
                 name='',
                 flags=0,
                 getter=None,
                 setter=None,
                 model=None,
                 queryFilter=None):
        self.__name = self.__name__ = name
        self.__model = model
        self.__schema = None
        self.__preload = None
        self.__getter = getter
        self.__setter = setter
        self.__query_filter = queryFilter
        self.__flags = self.Flags.fromSet(flags) if isinstance(flags, set) else flags

    def __call__(self, record, useMethod=True, **context):
        if self.__getter and useMethod:
            return self.__getter(record, **context)
        else:
            if not record.isRecord():
                return orb.Collection()
            else:
                collection = self.collect(record, **context)

                # preload the results
                if isinstance(collection, orb.Collection):
                    cache = record.preload(projex.text.underscore(self.name()))
                    collection.preload(cache, **context)

                    if self.testFlag(self.Flags.Unique):
                        return collection.first()
                    else:
                        return collection
                else:
                    return collection

    def copy(self):
        out = type(self)(name=self.__name, flags=self.__flags)
        out.setSchema(self.schema())
        return out

    def collect(self, record, **context):
        if self.__getter:
            return self.__getter(record, **context)
        raise NotImplementedError

    def collectExpand(self, query, parts, **context):
        raise NotImplementedError

    def queryFilter(self, function=None):
        """
        Defines a decorator that can be used to filter
        queries.  It will assume the function being associated
        with the decorator will take a query as an input and
        return a modified query to use.

        :usage

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.queryFilter()
                def objectsFilter(cls, query, **context):
                    return orb.Query()

        :param function: <callable>

        :return: <wrapper>
        """
        if function is not None:
            self.__query_filter = function
            return function

        def wrapper(func):
            self.__query_filter = func
            return func
        return wrapper

    def queryFilterMethod(self):
        """
        Returns the actual query filter method, if any,
        that is associated with this collector.

        :return: <callable>
        """
        return self.__query_filter

    def getter(self, function=None):
        if function is not None:
            self.__getter = function
            return function

        def wrapper(func):
            self.__getter = func
            return func
        return wrapper

    def gettermethod(self):
        return self.__getter

    def flags(self):
        return self.__flags

    def model(self):
        if isinstance(self.__model, (str, unicode)):
            schema = orb.system.schema(self.__model)
            if schema is not None:
                return schema.model()
            else:
                raise orb.errors.ModelNotFound(schema=self.__model)
        else:
            return self.__model

    def name(self):
        return self.__name

    def schema(self):
        return self.__schema

    def setter(self, function=None):
        if function is not None:
            self.__setter = function
            return function

        def wrapper(func):
            self.__setter = func
            return func

        return wrapper

    def settermethod(self):
        return self.__setter

    def setName(self, name):
        self.__name = self.__name__ = name

    def setSchema(self, schema):
        self.__schema = schema

    def setFlags(self, flags):
        self.__flags = flags

    def testFlag(self, flag):
        return (self.__flags & flag) > 0
