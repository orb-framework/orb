#!/usr/bin/python

""" Defines an piping system to use when accessing multi-to-multi records. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from xml.etree import ElementTree

orb = LazyModule('orb')


class Pipe(object):
    """ 
    Defines a piped way to lookup information from a database.
    Creating an Pipe generates an object that works like a method, however
    has a preset query built into it allowing multi-to-multi connections
    """
    def __init__( self, 
                  name              = '', 
                  pipeReference     = '',
                  sourceColumn      = '',
                  targetReference   = '',
                  targetColumn      = '',
                  cached            = False,
                  cachedExpires     = 0,
                  referenced        = False):
        
        self.__name__           = name
        self._pipeReference     = pipeReference
        self._pipeTable         = None
        self._sourceColumn      = sourceColumn
        self._targetReference   = targetReference
        self._targetTable       = None
        self._targetColumn      = targetColumn
        self._cached            = cached
        self._cachedExpires     = cachedExpires
        self._referenced        = referenced
        self._cache             = {}
    
    def __call__(self, record, **options):
        # return a blank piperecordset
        if not record.isRecord():
            return orb.PipeRecordSet([], record)
        
        reload = options.get('reload', False)
        pipeTable = self.pipeReferenceModel()
        targetTable = self.targetReferenceModel()
        if None in (pipeTable, targetTable):
            return orb.PipeRecordSet([], record)
        
        # update the caching information
        pipe_cache = self.cache(pipeTable)
        target_cache = self.cache(targetTable)
        cache_key = (id(record),
                     hash(orb.LookupOptions(**options)),
                     id(options.get('db', record.database())))
        
        # ensure neither the pipe nor target table have expired their caches
        if not reload and \
           pipe_cache and not pipe_cache.isExpired(cache_key) and \
           target_cache and not target_cache.isExpired(cache_key):
            return pipe_cache.value(cache_key)
        
        # create the query for the pipe
        q  = orb.Query(targetTable) == orb.Query(pipeTable, self._targetColumn)
        q &= orb.Query(pipeTable, self._sourceColumn) == record
        
        if 'where' in options:
            options['where'] = q & options['where']
        else:
            options['where'] = q
        
        # generate the new record set
        records = targetTable.select(**options)
        rset = orb.PipeRecordSet(records,
                                 record,
                                 pipeTable,
                                 self._sourceColumn,
                                 self._targetColumn)
        
        if pipe_cache:
            pipe_cache.setValue(cache_key, rset)
        if target_cache:
            target_cache.setValue(cache_key, rset)
        
        return rset
    
    def cache(self, table):
        """
        Returns the cache for the inputed table.
        
        :param      table | <subclass of orb.Table>
        
        :return     <orb.TableCache> || None
        """
        if not self.cached():
            return None
        
        elif not table in self._cache:
            cache = orb.TableCache(table, self._cachedExpires)
            self._cache[table] = cache
            return cache
        
        else:
            return self._cache[table]
    
    def cached(self):
        """
        Returns whether or not the results for this index should be cached.
        
        :return     <bool>
        """
        return self._cached
    
    def cachedExpires(self):
        """
        Returns the time in seconds to store the cached results for this pipe.
        
        :return     <int>
        """
        return self._cachedExpires
    
    def isReferenced(self):
        """
        Returns whether or not this
        """
        return self._referenced
    
    def name(self):
        """
        Returns the name of this index.
        
        :return     <str>
        """
        return self.__name__
    
    def pipeReference(self):
        return self._pipeReference
    
    def pipeReferenceModel(self):
        if self._pipeTable is None:
            pipeTable = orb.Orb.instance().model(self._pipeReference)
            if not self._targetReference and pipeTable:
                col = pipeTable.schema().column(self._targetColumn)
                self._targetReference = col.reference()
            
            self._pipeTable = pipeTable
        
        return self._pipeTable
    
    def setCached(self, state):
        """
        Sets whether or not this index should cache the results of its query.
        
        :param      state | <bool>
        """
        self._cached = state
    
    def setCachedExpires(self, seconds):
        """
        Sets the time in seconds to store the cached results for this pipe
        set.
        
        :param      seconds | <int>
        """
        self._cachedExpires = seconds
    
    def setName(self, name):
        """
        Sets the name for this index to this index.
        
        :param      name    | <str>
        """
        self.__name__ = nstr(name)
    
    def setPipeReference(self, reference):
        self._pipeReference = reference
    
    def setSourceColumn(self, column):
        self._sourceColumn = column
    
    def setTargetColumn(self, column):
        self._targetColumn = column
    
    def setTargetReference(self, reference):
        self._targetReference = reference
    
    def sourceColumn(self):
        return self._sourceColumn
    
    def targetColumn(self):
        return self._targetColumn
    
    def targetReference(self):
        return self._targetReference
    
    def targetReferenceModel(self):
        if self._targetTable is None:
            self._targetTable = orb.Orb.instance().model(self._targetReference)
        return self._targetTable

    def toolTip(self):
        return '<h1>{0} <small>Pipe</small></h1>'.format(self.name())

    def toXml(self, xparent):
        """
        Saves the index data for this column to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xpipe = ElementTree.SubElement(xparent, 'pipe')
        xpipe.set('name',            self.name())
        xpipe.set('pipeReference',   self._pipeReference)
        xpipe.set('sourceColumn',    self._sourceColumn)
        xpipe.set('targetReference', self._targetReference)
        xpipe.set('targetColumn',    self._targetColumn)
        xpipe.set('cached',  nstr(self.cached()))
        xpipe.set('expires', nstr(self._cachedExpires))
        
        return xpipe
    
    @staticmethod
    def fromXml(xpipe, referenced=False):
        """
        Generates an index method descriptor from xml data.
        
        :param      xindex  | <xml.etree.Element>
        
        :return     <Index> || None
        """
        pipe = Pipe(referenced=referenced)
        pipe.setName(xpipe.get('name', ''))
        pipe.setPipeReference(xpipe.get('pipeReference', ''))
        pipe.setSourceColumn(xpipe.get('sourceColumn', ''))
        pipe.setTargetReference(xpipe.get('targetReference', ''))
        pipe.setTargetColumn(xpipe.get('targetColumn', ''))
        pipe.setCached(xpipe.get('cached') == 'True')
        pipe.setCachedExpires(int(xpipe.get('expires', pipe._cachedExpires)))
        
        return pipe

