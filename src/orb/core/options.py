#!/usr/bin/python

"""
Defines the different options that can be used throughout the system.  Often,
classes and methods will accept a variable set of keyword arguments.  As
opposed to hardcoding these options everywhere and updating them, they
will map to one of the classes defined in this module.
"""

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

from projex.text import nativestring as nstr
from projex.lazymodule import LazyModule

orb = LazyModule('orb')


class DatabaseOptions(object):
    """"
    Defines a unique instance of information that will be bundled when
    calling different methods within the connections class.
    
    The DatabaseOptions class will accept a set of keyword arguments to
    control how the action on the database will be affected.  The options are:
    
    :param      namespace       | <str> || None (default: None)
                flags           | <orb.DatabaseFlags> (default: 0)
                dryRun          | <bool> (default: False) | When True, the database operation will only log, not actually perform any actions.
                useCache        | <bool> (default: False)
                inflateRecords  | <bool> (default: True) | When True, inflated <orb.Table> instances will be returned.  When False, the raw result is returned.
                autoIncrement   | <bool> (default: True)
                force           | <bool> (default: False)
                deleteFlags     | <orb.DeleteFlags> (default: all)
    """
    def __init__(self, **kwds):
        self.defaults = {'namespace': None,
                         'flags': 0,
                         'dryRun': False,
                         'useCache': False,
                         'inflateRecords': True,
                         'autoIncrement': True,
                         'force': False,
                         'locale': orb.system.locale(),
                         'deleteFlags': orb.DeleteFlags.all()}

        self.locale             = kwds.get('locale') or orb.system.locale()
        self.namespace          = kwds.get('namespace')
        self.flags              = kwds.get('flags', 0)
        self.dryRun             = kwds.get('dryRun', False)
        self.useCache           = kwds.get('useCache', False)
        self.inflateRecords     = kwds.get('inflated', 
                                  kwds.get('inflateRecords', True))
        self.autoIncrement      = kwds.get('autoIncrement', True)
        self.force              = kwds.get('force', False)
        self.deleteFlags        = kwds.get('deleteFlags', orb.DeleteFlags.all())

    def __str__(self):
        """
        Returns a string for this instance.
        
        :return     <str>
        """
        opts = []
        for key, default in self.defaults.items():
            val = getattr(self, key)
            if val == default:
                continue
            
            opts.append('{0}:{1}'.format(key, val))
        
        return '<DatabaseOptions {0}>'.format(' '.join(opts))
    
    def __hash__(self):
        """
        Returns a hash representation for this instance.
        
        :return     <hash>
        """
        return hash(nstr(self))

    def copy(self):
        """
        Reutrns a copy of this database option set.

        :return     <orb.DatabaseOptions>
        """
        return DatabaseOptions(**self.toDict())

    def isNull(self):
        """
        Returns whether or not this option set has been modified.
        
        :return     <bool>
        """
        for key, default in self.defaults.items():
            val = getattr(self, key)
            if val != default:
                return False
        return True

    def update(self, options):
        """
        Updates this lookup set with the inputed options.

        :param      options | <dict>
        """
        self.__dict__.update(options)

    def toDict(self):
        """
        Returns a dictionary representation of this database option set.
        
        :return     <dict>
        """
        return {
            'namespace': self.namespace,
            'flags': self.flags,
            'dryRun': self.dryRun,
            'useCache': self.useCache,
            'inflateRecords': self.inflateRecords,
            'deleteFlags': self.deleteFlags,
            'locale': self.locale
        }
    
    @staticmethod
    def fromDict(data):
        """
        Returns a new lookup options instance based on the inputed data
        dictionary.
        
        :param      data | <dict>
        
        
        :return     <LookupOptions>
        """
        return DatabaseOptions(**data)

#------------------------------------------------------------------------------

class LookupOptions(object):
    """
    Defines a unique instance of information that will be bundled when
    calling different query based methods in the connection class.
    
    The LookupOptions class will accept a set of keyword arguments to
    control how the action on the database will be affected.  The options are:
    
    :param      columns       | [<str>, ..] || None (default: None) | When provided, only the selected columns will be returned.
                where         | <orb.Query> || <orb.QueryCompound> || None (default: None)
                order         | [(<str> column, 'asc'/'desc'), ..] || None (default: None)
                start         | <int> || None (default: None)
                limit         | <int> || None (default: None)
                distinct      | <bool> (default: False)
                locale        | <str> || None | (default: None)
    """
    def __init__(self, **kwds):
        self.columns  = kwds.get('columns', None)
        self.where    = kwds.get('where', None)
        self.order    = kwds.get('order', None)
        self.start    = kwds.get('start', None)
        self.limit    = kwds.get('limit', None)
        self.distinct = kwds.get('distinct', False)
        self.expand   = kwds.get('expand', None)

        # ensure that the list is not modified
        if self.columns is not None:
            self.columns = list(self.columns)
        
        # make sure we have a valid query
        if self.where is not None and self.where.isNull():
            self.where = None

    def __str__(self):
        """
        Returns a string for this instance.
        
        :return     <str>
        """
        opts = []
        for key in ('columns',
                    'where',
                    'order',
                    'start',
                    'limit',
                    'expand'):
            val = getattr(self, key)
            if val is None:
                continue
            
            if orb.Query.typecheck(val) or orb.QueryCompound.typecheck(val):
                val = hash(val.toXmlString())
            
            opts.append('{0}:{1}'.format(key, val))
        
        if self.distinct:
            opts.append('distinct:True')
        
        return '<LookupOptions {0}>'.format(' '.join(opts))
    
    def __hash__(self):
        """
        Returns a hash representation for this instance.
        
        :return     <hash>
        """
        return hash(nstr(self))

    def copy(self):
        """
        Reutrns a copy of this database option set.

        :return     <orb.DatabaseOptions>
        """
        return LookupOptions(
            columns=self.columns[:] if self.columns else None,
            where=self.where.copy() if self.where is not None else None,
            order=self.order[:] if self.order else None,
            start=self.start,
            limit=self.limit,
            distinct=self.distinct,
            expand=self.expand[:] if self.expand else None
        )

    def isNull(self):
        """
        Returns whether or not this lookup option is NULL.
        
        :return     <bool>
        """
        for key in ('columns',
                    'where',
                    'order',
                    'start',
                    'limit',
                    'distinct',
                    'expand'):
            if getattr(self, key):
                return False
        
        return True

    def update(self, options):
        """
        Updates this lookup set with the inputed options.

        :param      options | <dict>
        """
        self.__dict__.update(options)

    def toDict(self):
        """
        Returns a dictionary representation of the lookup options.
        
        :return     <dict>
        """
        out = {}
        if self.columns:
            out['columns'] = self.columns[:]
        if self.where:
            out['where'] = self.where.toDict()
        if self.order:
            out['order'] = self.order[:]
        if self.start:
            out['start'] = self.start
        if self.limit:
            out['limit'] = self.limit
        if self.expand:
            out['expand'] = self.expand[:]
        return out
    
    @staticmethod
    def fromDict(data):
        """
        Returns a new lookup options instance based on the inputed data
        dictionary.
        
        :param      data | <dict>
        
        
        :return     <LookupOptions>
        """
        kwds = {}
        kwds.update(data)
        if 'where' in data:
            kwds['where'] = orb.Query.fromDict(data['where'])
        
        return LookupOptions(**kwds)

