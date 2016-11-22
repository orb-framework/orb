"""
Defines the main Table class that will be used when developing
database classes.
"""

import demandimport

from collections import defaultdict

with demandimport.enabled():
    import orb

# utility methods
# -------


def create_new_schema(name, attrs, base_models):
    """
    Creates a new schema based on the given attributes.  This method will
    actively modify the attributes passed in.

    :param name: <str>
    :param attrs: <dict> (io variable)
    :param base_models: [subclass of <orb.Model>, ..]

    :return: <orb.Schema>
    """
    if base_models:
        inherited_schema = base_models[0].schema()

        # propagate inheritable schema properties
        if inherited_schema:
            attrs.setdefault('__group__', inherited_schema.group())
            attrs.setdefault('__database__', inherited_schema.database())
            attrs.setdefault('__namespace__', inherited_schema.namespace())
            attrs.setdefault('__id__', inherited_schema.id_column().name())
            attrs.setdefault('__system__', inherited_schema.system())
    else:
        inherited_schema = None

    system = attrs.pop('__system__', None) or orb.system

    # create the new schema
    schema = orb.Schema(
        name,
        dbname=attrs.pop('__dbname__', ''),
        group=attrs.pop('__group__', ''),
        display=attrs.pop('__display__', ''),
        database=attrs.pop('__database__', ''),
        namespace=attrs.pop('__namespace__', ''),
        flags=attrs.pop('__flags__', 0),
        id_column=attrs.pop('__id__', 'id'),
        system=system
    )

    if inherited_schema:
        schema.set_inherits(inherited_schema.name())

    # automatically register the model to the system
    if attrs.pop('__register__', True):
        system.register(schema)

    return schema


def get_base_types(bases):
    """
    Filters down the bases into mixins and models.

    :return: [subclass of <orb.Model>, ..], [subclass of <orb.ModelMixin>, ..]
    """
    base_models = []
    base_mixins = []
    for base in bases:
        if issubclass(base, orb.Model):
            base_models.append(base)
        elif issubclass(base, orb.ModelMixin):
            base_mixins.append(base)
    return base_models, base_mixins


def generate_schema(name, attrs, bases):
    """
    Generates a new schema for the given attributes and base models.  This will
    create a new <orb.Schema> object and return it.

    :param name: <str>
    :param attrs: <dict> (io variable)

    :return: <orb.Schema>
    """
    # check for schema object
    schema = attrs.pop('__schema__', None)

    base_models, base_mixins = get_base_types(bases)

    # create one if not found
    if schema is None:
        schema = create_new_schema(name, attrs, base_models)

    # calculate additional objects from the attributes
    schema_objects = set()

    process_mixins(schema_objects, base_mixins)
    process_attrs(schema_objects, attrs)

    # register the schema attributes to the schema
    register_schema_objects(schema, schema_objects)

    return schema


def process_attrs(store, attrs):
    """
    Removes schema attributes from the attribute dictionary and moves them into
    the store.  This method will actively modify both the given store and the
    given attrs dictionary.

    :param store: <dict> (io variable)
    :param attrs: <dict> (io variable)
    """
    for key, value in attrs.items():
        if store_schema_object(store, key, value):
            if not isinstance(attrs[key], orb.Index):
                attrs.pop(key)
            else:
                attrs[key] = classmethod(attrs[key])
        else:
            store_virtual_schema_object(store, key, value)


def process_mixins(store, base_mixins):
    """
    Goes through a list of model mixin classes and extracts the
    schema objects from them.

    :param store: <set>
    :param base_mixins: [subclass of <orb.ModelMixin>, ..]
    """
    for mixin in base_mixins:
        try:
            orb_props = mixin.__orb__
        except AttributeError:
            orb_props = {}
            for key, value in vars(mixin).items():
                # store a copy of the mixin's schema object
                # so that it is unique per sub-class (copies
                # column, indexes, etc. vs. references them)
                if store_schema_object(store, key, value):
                    orb_props[key] = value.copy()

                    # remove this object from the mixin so
                    # that it is not accessed directly later
                    delattr(mixin, key)

                # add a virtual (method) based schema object
                # to the schema objects - but keep the key
                # because it is actually a virtual method that
                # is needed
                elif store_virtual_schema_object(store, key, value):
                    orb_props[key] = value.__orb__.copy()

            # after the first time processing a mixin's orb objects
            # store them for future access
            mixin.__orb__ = orb_props
        else:
            for name, schema_object in orb_props.items():
                store_schema_object(store, name, schema_object.copy())


def register_schema_objects(schema, store):
    """
    Registers each of the schema objects to this schema.

    :param schema: <orb.Schema>
    :param store: <set>
    """
    for schema_object in store:
        schema.register(schema_object)


def store_schema_object(store, name, schema_object):
    """
    Adds the schema object to the object store with the
    object's name.

    :param store: <set>
    :param name: <str>
    :param schema_object: <orb.Column> or <orb.Index> or <orb.Collector>

    :return: <bool>
    """
    if isinstance(schema_object, (orb.Column, orb.Index, orb.Collector)):
        schema_object.set_name(name)
        store.add(schema_object)
        return True
    else:
        return False


def store_virtual_schema_object(store, name, method):
    """
    Stores a virtual object as a schema object when defined.

    :param store: <set>
    :param name: <str>
    :param method: <callable>

    :return: <bool>
    """
    # determine if the object
    if isinstance(method, classmethod):
        method = method.__func__
        is_static = True
    else:
        is_static = False

    # check if the value contains orb information
    if not hasattr(method, '__orb__'):
        return False
    else:
        schema_object = method.__orb__

        if is_static:
            new_flags = schema_object.flags() | schema_object.Flags.Static
            schema_object.set_flags(new_flags)

        return store_schema_object(store, schema_object.name(), schema_object)


# ----------


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
        :param      attrs       <dict>

        :return     <type>
        """
        # check to see if this class is being used as a model type
        if not attrs.pop('__model__', True):
            return super(MetaModel, mcs).__new__(mcs, name, bases, attrs)

        # otherwise define a new model type
        else:
            # generate the schema for this class
            schema = generate_schema(name, attrs, bases)

            # store the schema on the model
            attrs['__schema__'] = schema

            # create the new model
            new_model = super(MetaModel, mcs).__new__(mcs, name, bases, attrs)

            # connect the schema and model together
            schema.set_model(new_model)

            # return the new model class
            return new_model

    def schema(cls):
        """
        Returns the schema that is associated with this model, if any is defined.

        :return: <orb.Schema> or None
        """
        return getattr(cls, '__schema__', None)

