"""
Defines the different options that can be used throughout the system.  Often,
classes and methods will accept a variable set of keyword arguments.  As
opposed to hard-coding these options everywhere and updating them, they
will map to one of the classes defined in this module.
"""

import copy
from projex.lazymodule import lazy_import

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
        'database': None,
        'distinct': False,
        'disinctOn': '',
        'dryRun': False,
        'expand': None,
        'force': False,
        'inflated': True,
        'limit': None,
        'locale': None,
        'namespace': '',
        'order': None,
        'page': None,
        'pageSize': None,
        'request': None,
        'returning': 'records',
        'start': None,
        'timezone': None,
        'where': None
    }

    def __eq__(self, other):
        return hash(self) == hash(other)

    def __ne__(self, other):
        return hash(self) != hash(other)

    def __hash__(self):
        return hash((k, self.raw_values[k] for k, v in self.Defaults.items() if self.raw_values[k] != v))

    def __init__(self, **kwds):
        # utilize values from another context
        other = kwds.pop('context', None)
        if other:
            ignore = ('columns', 'where')
            # extract expandable information
            for k, v in copy.deepcopy(other.raw_values).items():
                if k not in ignore:
                    kwds.setdefault(k, v)

            # merge where queries
            where = other.where
            if where is not None:
                q = orb.Query()
                q &= where
                q &= kwds.get('where')
                kwds['where'] = q

            # merge column queries
            columns = other.columns
            if columns is not None:
                kwds['columns'] = columns + [col for col in kwds.get('columns', []) if not col in columns]

        self.__dict__['raw_values'] = {k: v for k, v in kwds.items() if k in self.Defaults}

    def __getattr__(self, key):
        try:
            return self.raw_values.get(key) or self.Defaults[key]
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        if not key in self.Defaults:
            raise AttributeError(key)
        else:
            self.raw_values[key] = value

    def __iter__(self):
        defaults = self.Defaults.copy()
        defaults.update(self.raw_values)
        for k, v in defaults.items():
            yield k, v

    def copy(self):
        """
        Returns a copy of this database option set.

        :return     <orb.Context>
        """
        return Context(**copy.deepcopy(self.raw_values))

    @property
    def expand(self):
        out = self.raw_values['expand']
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

    def expandtree(self):
        expand = self.expand
        if not expand:
            return {}

        def build_tree(tree, name):
            name, _, remain = name.rpartition('.')

            tree.setdefault(name, {})
            if remain:
                build_tree(tree[name], remain)

        tree = {}
        for branch in expand:
            build_tree(tree, branch)
        return tree

    def isNull(self):
        """
        Returns whether or not this option set has been modified.

        :return     <bool>
        """
        return len(self.raw_values) != 0

    def items(self):
        defaults = self.Defaults.copy()
        defaults.update(self.raw_values)
        return defaults.items()

    @property
    def order(self):
        out = self.raw_values['order']
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
        return self.raw_values['pageSize'] or self.raw_values['limit']

    @property
    def start(self):
        if self.raw_values['page'] is not None:
            return (self.raw_values['page'] - 1) * self.limit
        else:
            return self.raw_values['start']

    def update(self, other):
        """
        Updates this lookup set with the inputted options.

        :param      other | <dict>
        """
        self.raw_values.update({k: v for k, v in other.items() if k in self.Defaults})
