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
        self.__name__ = column.getter()

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
        self.__name__ = column.setter()

    def __call__(self, record, value, **context):
        """
        Calls the setter method for the inputted database record.

        :param      record      <Table>
                    value       <variant>
        """
        return record.set(self.column, value, useMethod=False, **context)


#----------------------------------------------------------------------

class orb_lookup_method(object):
    """ Defines a reverse lookup method for lookup up relations. """

    def __init__(self, column):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the ModelType class when
        generating column methods on a model.
        """
        self.column = column
        self.__name__ = column.reverseInfo().name
        self.__lookup__ = True

    def __call__(self, record, **context):
        """
        Calls the getter lookup method for the database record.

        :param      record      <Table>
        """
        model = self.column.schema().model()
        if not record.isRecord():
            return None if self.column.testFlag(self.column.Flags.Unique) else orb.Collection()
        else:
            q = orb.Query(self.column) == record
            context['where'] = q & context.get('where')
            cache = record.preload(projex.text.underscore(self.__name__))
            records = model.select(**context)
            records.preload(cache, **context)
            return records.first() if self.column.testFlag(self.column.Flags.Unique) else records

# -----------------------------------------------------------------------------


class MetaModel(type):
    """
    Defines the table Meta class that will be used to dynamically generate
    Table class types.
    """
    ReverseCache = defaultdict(list)

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
        schema_attrs = attrs.pop('__orb__', {})
        if schema_attrs.get('bypass'):
            return super(MetaModel, mcs).__new__(mcs, name, bases, attrs)
        else:
            mixins = [base for base in bases if issubclass(base, orb.ModelMixin)]
            inherits = [base for base in bases if issubclass(base, orb.Model)]
            if not inherits:
                return super(MetaModel, mcs).__new__(mcs, name, bases, attrs)

            # define mixin options
            columns = {}
            indexes = {}
            pipes = {}
            views = {}

            items = [item for mixin in mixins for item in vars(mixin).items()] + attrs.items()

            # define inherited options
            for key, value in items:
                if isinstance(value, orb.Column):
                    col = value
                    col.setName(key)
                    columns[key] = col

                    # create a column index
                    col_index = col.index()
                    if col_index:
                        indexes[col_index.name] = orb.Index([col.name()], name=col_index.name, unique=col.testFlag(col.Flags.Unique))

                elif isinstance(value, orb.Index):
                    index = value
                    index.setName(key)
                    indexes[key] = index

                elif isinstance(value, orb.Pipe):
                    pipe = value
                    pipe.setName(key)
                    pipes[key] = pipe

                else:
                    try:
                        if issubclass(value, orb.View):
                            views[key] = value
                    except TypeError:
                        pass

            new_attrs = {k: v for k, v in attrs.items() if
                         k not in columns and
                         k not in indexes and
                         k not in pipes and
                         k not in views}

            # create the schema
            schema = orb.Schema(name, **schema_attrs)
            schema.setColumns(columns)
            schema.setIndexes(indexes)
            schema.setPipes(pipes)
            schema.setViews(views)

            if inherits:
                inherited_schema = inherits[0].schema()
                if inherited_schema:
                    schema.setInherits(inherited_schema.name())

            new_model = super(MetaModel, mcs).__new__(mcs, name, bases, new_attrs)
            setattr(new_model, '_{0}__schema'.format(name), schema)

            # create class methods for indexes
            for key, index in indexes.items():
                index.setSchema(schema)

                if not hasattr(new_model, key):
                    setattr(new_model, key, classmethod(index))

            # create instance methods for pipes
            for key, pipe in pipes.items():
                pipe.setSchema(schema)

                if not hasattr(new_model, key):
                    pipemethod = instancemethod(pipe, None, new_model)
                    setattr(new_model, key, pipemethod)

            # create instance methods for columns
            for key, column in columns.items():
                column.setSchema(schema)

                # create the getter method
                getter_name = column.getter()
                if getter_name and not hasattr(new_model, getter_name):
                    gmethod = orb_getter_method(column=column)
                    getter = instancemethod(gmethod, None, new_model)
                    setattr(new_model, getter_name, getter)

                # create the setter method
                setter_name = column.setter()
                if setter_name and not (column.testFlag(column.Flags.ReadOnly) or hasattr(new_model, setter_name)):
                    smethod = orb_setter_method(column=column)
                    setter = instancemethod(smethod, None, new_model)
                    setattr(new_model, setter_name, setter)

                # create reverse lookups
                if isinstance(column, orb.ReferenceColumn):
                    rev = column.reverseInfo()
                    if rev:
                        lookup = orb_lookup_method(column=column)

                        def get_ref(model):
                            if model is None:
                                return None
                            elif getattr(model, '_{0}__schema'.format(model.schema().name()), None):
                                return model
                            else:
                                for base in model.__bases__:
                                    ref = get_ref(base)
                                    if ref:
                                        return base
                                return None

                        ref_model = get_ref(column.referenceModel())
                        if ref_model:
                            ilookup = instancemethod(lookup, None, ref_model)
                            setattr(ref_model, rev.name, ilookup)
                        else:
                            MetaModel.ReverseCache[column.reference()].append((rev.name, lookup))

            # load reversed methods
            lookups = MetaModel.ReverseCache.pop(name, [])
            for rev_name, lookup in lookups:
                ilookup = instancemethod(lookup, None, new_model)
                setattr(new_model, rev_name, ilookup)

            # register the class to the system
            setattr(new_model, '_{0}__schema'.format(schema.name()), schema)
            schema.setModel(new_model)
            orb.system.register(schema)

            return new_model
