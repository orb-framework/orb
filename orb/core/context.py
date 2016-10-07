"""
Defines the different options that can be used throughout the system.  Often,
classes and methods will accept a variable set of keyword arguments.  As
opposed to hard-coding these options everywhere and updating them, they
will map to one of the classes defined in this module.
"""

import copy
import demandimport
import threading

from collections import defaultdict

from ..utils.locks import (
    ReadWriteLock,
    ReadLocker,
    WriteLocker
)

with demandimport.enabled():
    import orb

# query / backend context properties
QUERY_DEFAULTS = {
    'columns': None,
    'database': None,
    'distinct': False,
    'expand': None,
    'limit': None,
    'order': None,
    'page': None,
    'pageSize': None,
    'namespace': '',
    'force_namespace': False,
    'start': None,
    'where': None
}

# general context properties
GENERAL_DEFAULTS = {
    'dryRun': False,
    'format': 'json',
    'force': False,
    'inflated': None,
    'locale': None,
    'returning': 'records',
    'useBaseQuery': True,
    'timezone': None
}

# unhashable context properties
FIXED_DEFAULTS = {
    'db': None,
    'system': None,
    'scope': None
}

# define the context default dictionary
DEFAULTS = {}
DEFAULTS.update(QUERY_DEFAULTS)
DEFAULTS.update(GENERAL_DEFAULTS)
DEFAULTS.update(FIXED_DEFAULTS)

# define context stack locking
_context_lock = ReadWriteLock()
_context_stack = defaultdict(list)


class Context(object):
    def __init__(self, **kw):
        self.__dict__['raw_values'] = {}

        # load inherited context properties
        tid = threading.currentThread().ident
        with ReadLocker(_context_lock):
            default_contexts = _context_stack[tid]

        for default_context in default_contexts:
            self.update(default_context)

        # update the properties for this context
        self.update(kw)

    def __getattr__(self, key):
        if key not in DEFAULTS:
            raise AttributeError
        else:
            return self.raw_values.get(key, DEFAULTS[key])

    def __hash__(self):
        keys = sorted(DEFAULTS.keys())

        hash_values = []
        for key in keys:
            if key in FIXED_DEFAULTS:
                continue
            elif key not in self.raw_values:
                # we need to keep the same spacing between
                # values in the tuple for the
                # hashing function to work
                hash_values.append(None)
            else:
                value = self.raw_values[key]

                # cannot hash a list or set, so convert
                # it to a tuple first
                if isinstance(value, (list, set)):
                    value = tuple(value)

                try:
                    hash_value = hash(value)
                except TypeError:
                    hash_value = unicode(value)

                hash_values.append(hash_value)

        # hash the list of keys
        return hash(tuple(hash_values))

    def __iter__(self):
        for k in DEFAULTS:
            try:
                yield k, getattr(self, k)
            except orb.errors.DatabaseNotFound:
                yield k, None

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __enter__(self):
        tid = threading.currentThread().ident
        with WriteLocker(_context_lock):
            _context_stack[tid].append(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        tid = threading.currentThread().ident
        with WriteLocker(_context_lock):
            _context_stack[tid].pop()

    def __ne__(self, other):
        return hash(self) != hash(other)

    def __setattr__(self, key, value):
        if not key in DEFAULTS:
            raise AttributeError(key)
        else:
            self.raw_values[key] = value

    def copy(self):
        """
        Creates a duplicate of this context and returns it.

        :return: <orb.Context>
        """
        props = {
            k: v if k in FIXED_DEFAULTS else copy.copy(v)
            for k, v in self.raw_values.items()
        }
        return Context(**props)

    @property
    def db(self):
        """
        Returns a database instance associated with this
        context.

        :return: <orb.Database>
        """
        try:
            return self.raw_values['db']
        except KeyError:
            db = self.system.database(self.database)
            if not db:
                raise orb.errors.DatabaseNotFound()
            return db

    def difference(self, other_context):
        """
        Returns a set of the keys for the different values between this
        context and the other one.

        :return: <set>
        """
        return {k for k, v in DEFAULTS.items() if self.raw_values.get(k) != other_context.raw_values.get(k)}

    @property
    def expand(self):
        """
        Normalizes the raw expand values that were provided
        to the context into being a list of string column
        names.

        :usage

            context = orb.Context(expand='user,group')
            assert context.expand == ['user', 'group']

            context = orb.Context(expand=['user', 'group'])
            assert context.expand == ['user', 'group'])

            context = orb.Context(expand={'user': {'username': {}}})
            assert context.expand == ['user', 'user.username']

        """
        out = self.raw_values.get('expand')
        if isinstance(out, set):
            return list(out)
        elif isinstance(out, (str, unicode)):
            return out.split(',')
        elif isinstance(out, dict):
            def expand_string(key, children):
                return [key] + [key + '.' + child
                                for value in [expand_string(k_, v_) for k_, v_ in children.items()]
                                for child in value]
            return [entry for item in [expand_string(k, v) for k, v in out.items()] for entry in item]
        else:
            return out

    def expandtree(self, model=None):
        """
        Goes through the expand options associated with this context and
        returns a trie of data.

        :param model: subclass of <orb.Model> or None

        :return: <dict>
        """
        if model and not self.columns:
            schema = model.schema()
            defaults = schema.columns(flags=orb.Column.Flags.AutoExpand).keys()
            defaults += schema.collectors(flags=orb.Collector.Flags.AutoExpand).keys()
        else:
            defaults = []

        expand = self.expand or defaults
        if not expand:
            return {}

        def build_tree(parts, tree):
            tree.setdefault(parts[0], {})
            if len(parts) > 1:
                build_tree(parts[1:], tree[parts[0]])

        tree = {}
        for branch in expand:
            build_tree(branch.split('.'), tree)

        return tree

    def isNull(self):
        """
        Returns whether or not this context set has been modified.

        :return: <bool>
        """
        if 'scope' in self.raw_values:
            return len(self.raw_values) == 1 and len(self.raw_values['scope']) == 0
        else:
            return len(self.raw_values) == 0

    def items(self):
        """
        Returns a list of key value paired tuples for the
        values represented in this context.

        :return: [(<str> key, <variant> value), ..]
        """
        return [(k, v) for k, v in self]

    @property
    def locale(self):
        """
        Returns the locale for this context, or the
        locale defined by the default system settings.

        :return: <str>
        """
        return self.raw_values.get('locale') or self.system.settings.default_locale

    @property
    def order(self):
        """
        Normalizes the order property for the context, returning
        order as a list of tuples containing the column and
        direction for the order.

        :usage

            context = orb.Context(order=[('name', 'asc')])
            assert context.order == [('name', 'asc')]

            context = orb.Context(order='+first_name,-last_name')
            assert context.order == [('first_name', 'asc'), ('last_name', 'desc')]

            context = orb.Context(order={('name', 'asc')})
            assert context.order == [('name', 'asc')]

        :return: [(<str> column, <str> direction), ..]
        """
        out = self.raw_values.get('order')
        if isinstance(out, set):
            return list(out)
        elif isinstance(out, (str, unicode)):
            return [(x.strip().strip('+-'), 'desc' if x.strip().startswith('-') else 'asc')
                    for x in out.split(',') if x]
        else:
            return out

    def reversed(self):
        """
        Reverses the ordering of this context and returns a new one.

        :return <orb.Context>
        """
        curr_order = self.order
        if curr_order:
            new_order = [(col, 'asc' if dir == 'desc' else 'desc')
                         for col, dir in self.order or []]
        else:
            new_order = None

        # generate the new context
        out_context = self.copy()
        out_context.order = new_order
        return out_context

    def schema_columns(self, schema):
        """
        Returns a list of columns for the given schema based on
        the columns defined in this context.

        :return: [<orb.Column>, ..]
        """
        return [schema.column(col) for col in self.columns or []]

    def sub_context(self, **context):
        """
        Generates a sub-context of this instance with only
        general context settings, ignoring the query
        based context parameters.

        :return: <orb.Context>
        """

        sub_context = {k: v for k, v in self.raw_values.items()
                       if k not in QUERY_DEFAULTS}

        sub_context.update(context)
        return orb.Context(**sub_context)

    @property
    def limit(self):
        """
        Calculates the limit based on the limit or pageSize properties.

        :return: <int> or None
        """
        return self.raw_values.get('pageSize') or self.raw_values.get('limit')

    @property
    def scope(self):
        """
        Returns the scope associated with this context.  If the raw scope
        is not defined then a new dictionary is created.

        :return: <dict>
        """
        self.raw_values.setdefault('scope', {})
        return self.raw_values['scope']

    @property
    def system(self):
        """
        Returns a global orb System instance associated with this context.
        If no system is defined for this specific context, the global `orb.system`
        object will be used.

        :return: <orb.System>
        """
        try:
            return self.raw_values['system']
        except KeyError:
            return orb.system

    @property
    def start(self):
        """
        Calculates the starting index that should be used for data retrieval,
        which is either deriven from the page information, or the given start
        index.

        :return: <int> or None
        """
        if self.raw_values.get('page') is not None:
            return (self.raw_values.get('page') - 1) * (self.limit or 0)
        else:
            return self.raw_values.get('start')

    @property
    def timezone(self):
        """
        Returns the timezone defined for this context.  If no timezone
        is directly associated with it, then the default system server timezone
        will be used.

        :return: <str>
        """
        return self.raw_values.get('timezone') or self.system.settings.server_timezone

    def update(self, other_context):
        """
        Updates this lookup set with the inputted options.

        :param      other_context | <dict> or <orb.Context>
        """
        # use the raw values of the other context
        if isinstance(other_context, orb.Context):
            other_context = other_context.raw_values

        elif not isinstance(other_context, dict):
            return

        # inherit from another base context
        if 'context' in other_context:
            self.update(other_context.pop('context'))

        ignore = ('where', 'columns', 'scope')

        # merge the where queries together
        if 'where' in other_context:
            other_where = other_context['where']
            if isinstance(other_where, dict):
                other_where = orb.Query.fromJSON(other_where)

            if 'where' in self.raw_values:
                self.raw_values['where'] &= other_where
            else:
                self.raw_values['where'] = other_where

        # merge the column definitions together
        if 'columns' in other_context:
            other_columns = other_context['columns']
            if isinstance(other_columns, (str, unicode)):
                other_columns = other_columns.split(',')

            if 'columns' in self.raw_values:
                self.raw_values['columns'] = list(self.raw_values['columns']) + other_columns
            else:
                self.raw_values['columns'] = list(other_columns)

        # merge the scope together
        if 'scope' in other_context:
            self.scope.update(other_context['scope'])

        # set the values for the other properties
        for k, v in other_context.items():
            if k not in ignore:
                self.raw_values[k] = v

        # validate values
        for field, minimum in (('start', 0), ('page', 1), ('limit', 1), ('pageSize', 1)):
            value = self.raw_values.get(field)
            if value is None:
                continue

            if type(value) != int or self.raw_values[field] < minimum:
                msg = '{0} needs to be an integer greater than or equal to {1}, got {2}'
                raise orb.errors.ContextError(msg.format(field, minimum, value))

