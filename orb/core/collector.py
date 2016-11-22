"""
Defines the base collector class type.  Collectors are defined on
models and are used to create lookups between records.
"""

import demandimport

from ..utils.enum import enum

with demandimport.enabled():
    import orb


class Collector(object):
    Flags = enum(
        'Unique',
        'Private',
        'ReadOnly',
        'Virtual',
        'Static',
        'AutoExpand'
    )

    def __init__(self,
                 model=None,
                 name='',
                 flags=0,
                 getter=None,
                 setter=None,
                 filter=None,
                 schema=None):
        # define custom properties
        self.__model = model
        self.__name = self.__name__ = name
        self.__flags = self.Flags.from_set(flags) if isinstance(flags, set) else flags
        self.__gettermethod = getter
        self.__settermethod = setter
        self.__filtermethod = filter
        self.__schema = schema

    def __call__(self, source_record, use_method=True, **context):
        """
        Executes the logic for collecting the records for this
        instance.  If there is a specific getter method associated
        with this collector, and the `use_method` property is True then
        that function is called.

        :param source_record: <orb.Model>
        :param use_method: <bool>
        :param context: <orb.Context>

        :return: <orb.Collection>
        """
        return self.collect(source_record, use_method=use_method, **context)

    def __eq__(self, other):
        return self is other

    def __ne__(self, other):
        return self is not other

    def __lt__(self, other):
        if self is other:
            return False
        elif isinstance(other, Collector):
            return self.name() < other.name()
        else:
            return False

    def __json__(self):
        """
        Serializes this collector object as the descriptor for
        it's meta data.

        :return: <dict>
        """
        try:
            model = self.model()
        except orb.errors.ModelNotFound:
            model_name = None
        else:
            model_name = model.schema().name() if model else None

        output = {
            'name': self.__name,
            'model': model_name,
            'flags': {k: True for k in self.Flags.to_set(self.__flags)}
        }
        return output

    def _collect(self, source_record, **context):
        """
        Protected method used to perform actual data collection from the backend.
        This method is abstract and will need to be implemented in a sub-class based
        on the requirements for that collector type.

        :param source_record: <orb.Model>
        :param context: <orb.Context>

        :return: <orb.Collection>
        """
        raise NotImplementedError

    def add_record(self, source_record, target_record, **context):
        """
        Adds a new record to the backend through this collector.  This method
        takes the source and target records as input and generates the relationship
        between them that will be required for future lookup by this collector.

        This is an abstractmethod and needs to be implemented by a sub-class for
        it to work.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context> descriptor

        :return: <orb.Model> generated relationship
        """
        raise NotImplementedError

    def copy(self, **kw):
        """
        Creates a copy of this collector.

        :param kw: <dict> keywords

        :return: <orb.Collector>
        """
        kw.setdefault('model', self.__model)
        kw.setdefault('name', self.__name)
        kw.setdefault('flags', self.__flags)
        kw.setdefault('getter', self.__gettermethod)
        kw.setdefault('setter', self.__settermethod)
        kw.setdefault('filter', self.__filtermethod)
        kw.setdefault('schema', self.__schema)
        return type(self)(**kw)

    def collection(self, source_record):
        """
        Returns a bound collection instance associated with this collector.

        :param source_record: <orb.Model>

        :return: <orb.Collection>
        """
        collection = orb.Collection()
        collection.bind_model(self.model())
        collection.bind_collector(self)
        collection.bind_source_record(source_record)
        return collection

    def collect(self, source_record, use_method=True, **context):
        """
        Collects the records that are related to the given source method through
        this collector.  If a gettermethod is defined for this collector, and the
        `use_method` is True, then that gettermethod will be called.  Otherwise
        the base logic for this collector is called.

        :param source_record: <orb.Model>
        :param use_method: <bool>
        :param context: <orb.Context> descriptor

        :return: <orb.Collection>
        """
        # run the gettermethod for this collector if specified
        # and available
        if self.__gettermethod is not None and use_method:
            return self.__gettermethod(source_record, **context)

        else:
            # ensure we have a valid record for collection
            if not (source_record and source_record.is_record()):
                return orb.Collection()

            # run the collect method to gather the records for this
            # context
            else:
                collection = self._collect(source_record, **context)

                # pre-load any data that was pulled from the source record for
                # this collector and auto-assign it to the resulting collection
                if isinstance(collection, orb.Collection):
                    record_cache = source_record.preloaded_data(self.name())
                    if record_cache:
                        collection.preload_data({'records': record_cache}, **context)

                    if self.test_flag(self.Flags.Unique):
                        return collection.first()
                    else:
                        return collection
                else:
                    return collection

    def collect_expand(self, query, parts, **context):
        """
        Adds the query needed to expand this collector into another
        sub-query.

        This method is abstract and needs to be re-implemented in a sub-class.

        :param query: <orb.Query>
        :param parts: [<str> sub-expand, ..]
        :param context: <orb.Context> descriptor

        :return: <orb.Query>
        """
        raise NotImplementedError

    def create_record(self, source_record, values, **context):
        """
        Creates a new record through this collector from the given source record with
        the given values.  This will generate a new model instance based on this collector's
        model type.  This method will also automatically create the relationship
        for the new record instance that is represented via the collector.

        :param source_record: <orb.Model>
        :param values: <dict>
        :param context: <orb.Context> descriptor

        :return: <orb.Model> generated model
        """
        raise NotImplementedError

    def delete_records(self, collection, **context):
        """
        Deletes the collection of records from the database.  This method
        is abstract and needs to be re-implemented in a sub-class.

        :param collection: <orb.Collection>
        :param context: <orb.Context> descriptor

        :return: [<orb.Model>, ..] processed, <int> count deleted
        """
        raise NotImplementedError

    def filter(self, function=None):
        """
        Decorator for wrapping a function to use as the filter method
        for this collector.  The filter method will generate queries
        to allow filtering based on this collector from the backend.

        :usage:

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.filter()
                def objects_filter(cls, query, **context):
                    return orb.Query()

        :param function: <callable>

        :return: <callable>
        """
        if function is not None:
            self.__filtermethod = function
            return function
        else:
            def wrapper(f):
                self.__filtermethod = f
                return f
            return wrapper

    def filtermethod(self):
        """
        Returns the actual query filter method, if any,
        that is associated with this collector.

        :return: <callable>
        """
        return self.__filtermethod

    def getter(self, function=None):
        """
        Decorator for wrapping a function to use as the getter method
        for this collector.  The getter method will return a collection
        of records that are associated with this method.  Usage of
        this system will override the default `collect` method for
        this collector.

        :usage:

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.getter()
                def get_objects(cls, **context):
                    return orb.Collection()

        :param function: <callable>

        :return: <callable>
        """
        if function is not None:
            self.__gettermethod = function
            return function
        else:
            def wrapper(f):
                self.__gettermethod = f
                return f
            return wrapper

    def gettermethod(self):
        """
        Returns the getter method associated with this collector.  When
        defined, the getter method will override the `collect` method used
        when gathering records for this collector.  This will override the
        values returned when using the `orb.Model.get` method on a collector.

        :return: <callable> or None
        """
        return self.__gettermethod

    def flags(self):
        """
        Returns the flags that have been set for this collector instance.

        :return: <int> Collector.Flags
        """
        return self.__flags

    def model(self):
        """
        Returns the model instance that is associated with this collector.
        If a string is associated with the model type for this collector,
        it is possible for this method to raise an `orb.errors.ModelNotFound`
        error.

        :return: <orb.Model>
        """
        if isinstance(self.__model, (str, unicode)):
            system = self.__schema.system() if self.__schema else orb.system
            self.__model = system.model(self.__model)
        return self.__model

    def name(self):
        """
        Returns the name of this collector.

        :return: <str>
        """
        return self.__name

    def remove_record(self, source_record, target_record, **context):
        """
        Removes the relationship between the source_record and target_record for this
        collector instance.

        :param source_record: <orb.Model>
        :param target_record: <orb.Model>
        :param context: <orb.Context>

        :return: <int> number of records removed
        """
        raise NotImplementedError

    def set_name(self, name):
        """
        Sets the name of this collector to the given string.

        :param name: <str>
        """
        self.__name = name

    def schema(self):
        """
        Returns the schema that is associated with this collector
        instance.

        :return: <orb.Schema>
        """
        return self.__schema

    def setter(self, function=None):
        """
        Decorator for wrapping a function to use as the setter method
        for this collector.  The setter method will take a collection
        of records and associate them with this collector instance.  Usage of
        this system will override the default `orb.Model.set` method for
        this collector.

        :usage:

            class MyModel(orb.Model):
                objects = orb.ReverseLookup('Object')

                @classmethod
                @objects.setter()
                def set_objects(cls, records, **context):
                    pass

        :param function: <callable>

        :return: <callable>
        """
        if function is not None:
            self.__settermethod = function
            return function
        else:
            def wrapper(f):
                self.__settermethod = f
                return f
            return wrapper

    def settermethod(self):
        """
        Returns the setter method associated with this collector.  This will override
        the action provided to the `orb.Model.set` method when setting values of a collector.

        :return: <callable> or None
        """
        return self.__settermethod

    def set_schema(self, schema):
        """
        Sets the schema that is associated with this collector
        instance.

        :param schema: <orb.Schema>
        """
        self.__schema = schema

    def set_flags(self, flags):
        """
        Sets the flag values for this collector to the given set of flags.

        :param flags: <int>
        """
        self.__flags = flags

    def test_flag(self, flag):
        """
        Checks to see if the given flag is defined for this
        collector.

        :param flag: <int>

        :return: <bool>
        """
        return (self.__flags & flag) > 0

    def update_records(self, source_record, records, **context):
        """
        Updates the source record with the new collection of related records based
        on this collector's required relationships.  This is an abstract method
        and needs to be implemented per sub-class.

        :param source_record: <orb.Model>
        :param records: <orb.Collection> new related records
        :param context: <orb.Context> descriptor
        """
        raise NotImplementedError