"""
Defines the main Table class that will be used when developing
database classes.
"""

# ------------------------------------------------------------------------------

import projex.text

from collections import defaultdict
from new import instancemethod
from projex.lazymodule import lazy_import

orb = lazy_import('orb')


# ------------------------------------------------------------------------------

class orb_getter_method(object):
    """ Creates a method for tables to use as a field accessor. """

    def __init__(self, column):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the ModelType class when
        generating column methods on a model.
        """
        self.column = column
        self.__name__ = column.getterName()

    def __call__(self, record, default=None, **context):
        """
        Calls the getter lookup method for the database record.

        :param      record      <Table>
        """
        return record.get(self.column, default=default, useMethod=False, **context)


#------------------------------------------------------------------------------

class orb_setter_method(object):
    """ Defines a method for setting database fields on a Table instance. """

    def __init__(self, column):
        """
        Defines the setter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the ModelType class when
        generating column methods on a model
        """
        self.column = column
        self.__name__ = column.setterName()

    def __call__(self, record, value, **context):
        """
        Calls the setter method for the inputted database record.

        :param      record      <Table>
                    value       <variant>
        """
        return record.set(self.column, value, useMethod=False, **context)


#----------------------------------------------------------------------


class MetaModel(type):
    """
    Defines the table Meta class that will be used to dynamically generate
    Table class types.
    """
    def __new__(mcs, name, bases, attrs):
        """
        Manages the creation of database model classes, reading
        through the creation attributes and generating table
        schemas based on the inputted information.  This class
        never needs to be expressly defined, as any class that
        inherits from the Table class will be passed through this
        as a constructor.

        :param      mcs         <MetaTable>
        :param      name        <str>
        :param      bases       <tuple> (<object> base,)
        :param      attrs       <dict> properties

        :return     <type>
        """
        # define orb attributes
        is_model = attrs.pop('__model__', True)
        if not is_model:
            return super(MetaModel, mcs).__new__(mcs, name, bases, attrs)
        else:
            mixins = [base for base in bases if issubclass(base, orb.ModelMixin)]
            inherits = [base for base in bases if issubclass(base, orb.Model)]

            if not inherits:
                return super(MetaModel, mcs).__new__(mcs, name, bases, attrs)

            # define mixin options
            columns = {}
            indexes = {}
            collectors = {}
            views = {}

            def store_property(key, value):
                if isinstance(value, orb.Column):
                    col = value
                    col.setName(key)
                    columns[key] = col
                    return True

                elif isinstance(value, orb.Index):
                    index = value
                    index.setName(key)
                    indexes[key] = index
                    return True

                elif isinstance(value, orb.Collector):
                    collector = value
                    collector.setName(key)
                    collectors[key] = collector
                    return True

                else:
                    try:
                        if issubclass(value, orb.View):
                            views[key] = value
                            return True
                    except TypeError:
                        pass

                return False

            # strip out mixin properties
            for mixin in mixins:
                try:
                    orb_props = mixin.__orb_properties__
                except AttributeError:
                    orb_props = {}
                    for key, value in vars(mixin).items():
                        if store_property(key, value):
                            orb_props[key] = value.copy()
                            delattr(mixin, key)
                    mixin.__orb_properties__ = orb_props
                else:
                    for key, value in orb_props.items():
                        store_property(key, value.copy())

            # strip out attribute properties
            for key, value in attrs.items():
                if store_property(key, value):
                    attrs.pop(key)
                elif hasattr(value, '__orb__'):
                    if isinstance(value.__orb__, orb.Column):
                        columns[value.__orb__.name()] = value.__orb__
                    elif isinstance(value.__orb__, orb.Collector):
                        collectors[value.__orb__.name()] = value.__orb__

            # check to see if a schema is already defined
            schema = attrs.pop('__schema__', None)

            # otherwise, create a new schema
            if schema is None:
                schema = orb.Schema(
                    name,
                    dbname=attrs.pop('__dbname__', ''),
                    display=attrs.pop('__display__', ''),
                    database=attrs.pop('__database__', ''),
                    namespace=attrs.pop('__namespace__', ''),
                    flags=attrs.pop('__flags__', 0)
                )
                schema.setColumns(columns)
                schema.setIndexes(indexes)
                schema.setCollectors(collectors)
                schema.setViews(views)

                if inherits:
                    inherited_schema = inherits[0].schema()
                    if inherited_schema:
                        schema.setInherits(inherited_schema.name())

            new_model = super(MetaModel, mcs).__new__(mcs, name, bases, attrs)
            setattr(new_model, '_{0}__schema'.format(name), schema)

            # create class methods for indexes
            for key, index in indexes.items():
                index.setSchema(schema)

                if not hasattr(new_model, key):
                    setattr(new_model, key, classmethod(index))

            # create instance methods for collectors
            for key, collector in collectors.items():
                collector.setSchema(schema)

                if not hasattr(new_model, key):
                    collectormethod = instancemethod(collector, None, new_model)
                    setattr(new_model, key, collectormethod)

            # create instance methods for columns
            for key, column in columns.items():
                column.setSchema(schema)

                # create the getter method
                getter_name = column.getterName()
                if getter_name and not hasattr(new_model, getter_name):
                    gmethod = column.gettermethod()
                    if gmethod is None:
                        gmethod = orb_getter_method(column=column)

                    getter = instancemethod(gmethod, None, new_model)
                    setattr(new_model, getter_name, getter)

                # create the setter method
                setter_name = column.setterName()
                if setter_name and not (column.testFlag(column.Flags.ReadOnly) or hasattr(new_model, setter_name)):
                    smethod = column.settermethod()
                    if smethod is None:
                        smethod = orb_setter_method(column=column)

                    setter = instancemethod(smethod, None, new_model)
                    setattr(new_model, setter_name, setter)

            # register the class to the system
            setattr(new_model, '_{0}__schema'.format(schema.name()), schema)
            schema.setModel(new_model)
            orb.system.register(schema)

            return new_model
