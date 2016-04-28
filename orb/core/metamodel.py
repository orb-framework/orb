"""
Defines the main Table class that will be used when developing
database classes.
"""

# ------------------------------------------------------------------------------

import projex.text

from collections import defaultdict

from projex.lazymodule import lazy_import

orb = lazy_import('orb')


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

                return False

            def store_virtual_property(key, value):
                if isinstance(value, classmethod):
                    value = value.__func__
                    is_static = True
                else:
                    is_static = False

                if hasattr(value, '__orb__'):
                    # update the static flag when needed
                    if is_static:
                        value.__orb__.setFlags(value.__orb__.flags() | value.__orb__.Flags.Static)

                    # store the virtual object
                    if isinstance(value.__orb__, orb.Column):
                        columns[value.__orb__.name()] = value.__orb__
                    elif isinstance(value.__orb__, orb.Collector):
                        collectors[value.__orb__.name()] = value.__orb__

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
                        else:
                            store_virtual_property(key, value)
                    mixin.__orb_properties__ = orb_props
                else:
                    for key, value in orb_props.items():
                        store_property(key, value.copy())

            # strip out attribute properties
            for key, value in attrs.items():
                if store_property(key, value):
                    attrs.pop(key)
                else:
                    store_virtual_property(key, value)

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
                    flags=attrs.pop('__flags__', 0),
                    idColumn = attrs.pop('__id__', 'id')
                )

                if inherits:
                    inherited_schema = inherits[0].schema()
                    if inherited_schema:
                        schema.setInherits(inherited_schema.name())

            new_model = super(MetaModel, mcs).__new__(mcs, name, bases, attrs)

            # register the class to the system
            setattr(new_model, '_{0}__schema'.format(schema.name()), schema)
            schema.setModel(new_model)
            orb.system.register(schema)

            # create class methods for indexes
            for index in indexes.values():
                schema.register(index)

            # create instance methods for collectors
            for collector in collectors.values():
                schema.register(collector)

            # create instance methods for columns
            for column in columns.values():
                schema.register(column)

            return new_model
