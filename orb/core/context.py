"""
Defines the different options that can be used throughout the system.  Often,
classes and methods will accept a variable set of keyword arguments.  As
opposed to hard-coding these options everywhere and updating them, they
will map to one of the classes defined in this module.
"""

import copy
import threading
from collections import defaultdict
from projex.lazymodule import lazy_import
from projex.locks import ReadWriteLock, WriteLocker, ReadLocker

orb = lazy_import('orb')

class Context(object):
    """"
    Defines a unique instance of information that will be bundled when
    calling different methods within the connections class.

    The Context class will accept a set of keyword arguments to
    control how the action on the database will be affected.  The options are:
    """
    Defaults = {
        'autoIncrementEnabled': True,
        'columns': None,
        'db': None,
        'database': None,
        'distinct': False,
        'dryRun': False,
        'expand': None,
        'format': 'json',
        'force': False,
        'inflated': None,
        'limit': None,
        'locale': None,
        'namespace': '',
        'forceNamespace': False,
        'order': None,
        'page': None,
        'pageSize': None,
        'scope': None,
        'returning': 'records',
        'start': None,
        'timezone': None,
        'where': None,
        'useBaseQuery': True
    }

    QueryFields = {
        'columns',
        'expand',
        'limit',
        'order',
        'page',
        'pageSize',
        'start',
        'where'
    }

    UnhashableOptions = {
        'db',
        'scope'
    }

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return hash(self) != hash(other)

    def __hash__(self):
        keys = sorted(self.Defaults.keys())

        hash_keys = []
        for key in keys:
            if key in self.__class__.UnhashableOptions:
                continue

            value = self.raw_values.get(key, self.__class__.Defaults[key])
            if isinstance(value, (list, set)):
                value = tuple(value)

            try:
                hash_value = hash(value)
            except TypeError:
                hash_value = unicode(value)

            hash_keys.append(hash_value)


        return hash(tuple(hash_keys))

    def __enter__(self):
        """
        Creates a scope where this context is default, so all calls made while it is in scope will begin with
        the default context information.

        :usage      |import orb
                    |with orb.Context(database=db):
                    |   user = models.User()
                    |   group = models.Group()

        :return:  <orb.Context>
        """
        self.pushDefaultContext(self)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.popDefaultContext()
        if exc_type:
            raise
        else:
            return self

    def __init__(self, **kwds):
        self.__dict__['raw_values'] = {}
        self.update(kwds)

    def __getattr__(self, key):
        try:
            return self.raw_values.get(key, self.Defaults[key])
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        if not key in self.Defaults:
            raise AttributeError(key)
        else:
            self.raw_values[key] = value

    def __iter__(self):
        for k in self.Defaults:
            yield k, getattr(self, k)

    def copy(self):
        """
        Returns a copy of this database option set.

        :return     <orb.Context>
        """
        properties = {}
        for key, value in self.raw_values.items():
            if key in self.UnhashableOptions:
                properties[key] = value
            else:
                properties[key] = copy.copy(value)

        return Context(**properties)

    @property
    def db(self):
        try:
            return self.raw_values['db']
        except KeyError:
            db = orb.system.database(self.database)
            if not db:
                raise orb.errors.DatabaseNotFound()
            return db

    @property
    def expand(self):
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

        :param model: subclass of <orb.Model> || None

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
        Returns whether or not this option set has been modified.

        :return     <bool>
        """
        check = self.raw_values.copy()
        scope = check.pop('scope', {})
        return len(check) == 0 and len(scope) == 0

    def items(self):
        return [(k, getattr(self, k)) for k in self.Defaults]

    @property
    def locale(self):
        return self.raw_values.get('locale') or orb.system.settings().default_locale

    @property
    def order(self):
        out = self.raw_values.get('order')
        if isinstance(out, set):
            return list(out)
        elif isinstance(out, (str, unicode)):
            return [(x.strip('+-'), 'desc' if x.startswith('-') else 'asc') for x in out.split(',') if x]
        else:
            return out

    def schemaColumns(self, schema):
        return [schema.column(col) for col in self.columns or []]

    @property
    def limit(self):
        return self.raw_values.get('pageSize') or self.raw_values.get('limit')

    @property
    def scope(self):
        out = self.raw_values.get('scope')
        return out if out is not None else {}

    @property
    def start(self):
        if self.raw_values.get('page') is not None:
            return (self.raw_values.get('page') - 1) * (self.limit or 0)
        else:
            return self.raw_values.get('start')

    @property
    def timezone(self):
        return self.raw_values.get('timezone') or orb.system.settings().server_timezone

    def update(self, other_context):
        """
        Updates this lookup set with the inputted options.

        :param      other_context | <dict> || <orb.Context>
        """
        # convert a context instance into a dictionary
        if isinstance(other_context, orb.Context):
            other_context = copy.copy(other_context.raw_values)

        ignore = ('where', 'columns', 'scope')
        inherit_kwds = {}
        inherit_scope = {}
        inherit_columns = []
        inherit_where = orb.Query()

        # update from the base context
        base_context = other_context.pop('context', None)
        if base_context is not None:
            inherit_kwds = base_context.raw_values

        # use the default contexts
        else:
            for default in self.defaultContexts():
                if default is not None:
                    # extract expandable information
                    for k, v in default.raw_values.items():
                        if k not in ignore:
                            inherit_kwds[k] = copy.copy(v)

                    # merge where queries
                    where = default.where
                    if where is not None:
                        inherit_where &= where

                    # merge column queries
                    columns = default.columns
                    if columns is not None:
                        inherit_columns += list(columns)

                    # merge scope
                    scope = default.scope
                    if scope:
                        inherit_scope.update(scope)

        # update the inherited kwds
        for k, v in inherit_kwds.items():
            other_context.setdefault(k, v)

        # update the inherited query
        if inherit_where:
            other_context.setdefault('where', orb.Query())
            other_context['where'] &= inherit_where

        # update the inherited columns
        if inherit_columns:
            other_context['columns'] = inherit_columns + (other_context.get('columns') or [])

        # update the inherited scope
        if inherit_scope:
            new_scope = {}
            new_scope.update(inherit_scope)
            new_scope.update(other_context.get('scope') or {})
            other_context['scope'] = new_scope

        # convert the columns to a list
        if 'columns' in other_context and isinstance(other_context['columns'], (str, unicode)):
            other_context['columns'] = other_context['columns'].split(',')

        # convert where to query
        where = other_context.get('where')
        if isinstance(where, dict):
            other_context['where'] = orb.Query.fromJSON(where)

        if isinstance(where, (orb.Query, orb.QueryCompound)):
            other_context['where'] &= self.where

        # validate values
        if other_context.get('start') is not None and (type(other_context['start']) != int or other_context['start'] < 0):
            msg = 'Start needs to be a positive number, got {0} instead'
            raise orb.errors.ContextError(msg.format(other_context.get('start)')))
        if other_context.get('page') is not None and (type(other_context['page']) != int or other_context['page'] < 1):
            msg = 'Page needs to be a number equal to or greater than 1, got {0} instead'
            raise orb.errors.ContextError(msg.format(other_context.get('page')))
        if other_context.get('limit') is not None and (type(other_context['limit']) != int or other_context['limit'] < 1):
            msg = 'Limit needs to be a number equal to or greater than 1, got {0} instead'
            raise orb.errors.ContextError(msg.format(other_context.get('limit')))
        if other_context.get('pageSize') is not None and (type(other_context['pageSize']) != int or other_context['pageSize'] < 1):
            msg = 'Page size needs to be a number equal to or greater than 1, got {0} instead'
            raise orb.errors.ContextError(msg.format(other_context.get('pageSize')))

        # update the raw values
        self.raw_values.update({k: v for k, v in other_context.items() if k in self.Defaults})

    @classmethod
    def defaultContexts(cls):
        defaults = getattr(cls, '_{0}__defaults'.format(cls.__name__), None)
        if defaults is None:
            defaults = defaultdict(list)
            lock = ReadWriteLock()
            setattr(cls, '_{0}__defaults'.format(cls.__name__), defaults)
            setattr(cls, '_{0}__defaultsLock'.format(cls.__name__), lock)
        else:
            lock = getattr(cls, '_{0}__defaultsLock'.format(cls.__name__))

        tid = threading.currentThread().ident
        with ReadLocker(lock):
            return defaults.get(tid) or []

    @classmethod
    def popDefaultContext(cls):
        defaults = getattr(cls, '_{0}__defaults'.format(cls.__name__), None)
        if defaults is None:
            defaults = defaultdict(list)
            lock = ReadWriteLock()
            setattr(cls, '_{0}__defaults'.format(cls.__name__), defaults)
            setattr(cls, '_{0}__defaultsLock'.format(cls.__name__), lock)
        else:
            lock = getattr(cls, '_{0}__defaultsLock'.format(cls.__name__))

        tid = threading.currentThread().ident
        with WriteLocker(lock):
            defaults[tid].pop()

    @classmethod
    def pushDefaultContext(cls, context):
        defaults = getattr(cls, '_{0}__defaults'.format(cls.__name__), None)
        if defaults is None:
            defaults = defaultdict(list)
            lock = ReadWriteLock()
            setattr(cls, '_{0}__defaults'.format(cls.__name__), defaults)
            setattr(cls, '_{0}__defaultsLock'.format(cls.__name__), lock)
        else:
            lock = getattr(cls, '_{0}__defaultsLock'.format(cls.__name__))

        tid = threading.currentThread().ident
        with WriteLocker(lock):
            defaults[tid].append(context)
