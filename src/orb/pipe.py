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

import datetime
import logging

from xml.etree import ElementTree

import orb
from projex.text import nativestring
from orb.recordset import RecordSet
from orb.query     import Query as Q

logger = logging.getLogger(__name__)

class PipeRecordSet(RecordSet):
    def __init__(self, 
                 records,
                 source,
                 pipeTable=None,
                 sourceColumn='',
                 targetColumn=''):
        
        super(PipeRecordSet, self).__init__(records)
        
        # define additional properties
        self._source = source
        self._pipeTable = pipeTable
        self._sourceColumn = sourceColumn
        self._targetColumn = targetColumn
    
    def addRecord(self, record, **options):
        """
        Adds a new record for this pipe set.  The inputed record should refer
        to the value for the target column and be an instance of the
        target type.
        
        :param      record | <orb.Table>
        
        :return     <orb.Table> || None | instance of the pipe table
        """
        # make sure we have a valid table and record
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return None
        
        # make sure we have a pipe table
        if not self._pipeTable:
            return None
        
        pipe = self._pipeTable
        unique = options.pop('uniqueRecord', True)
        
        if unique:
            q  = Q(pipe, self._sourceColumn) == self._source
            q &= Q(pipe, self._targetColumn) == record
            
            if pipe.selectFirst(where = q):
                return None
        
        options[self._sourceColumn] = self._source
        options[self._targetColumn] = record
        
        link = pipe(**options)
        link.commit()
        
        return link
    
    def clear(self, **options):
        """
        Clears all the records through the pipeset based on the inputed
        parameters.
        
        :return     <int>
        """
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        q  = Q(pipe, self._sourceColumn) == self._source
        
        where = options.pop('where', None)
        q &= where
        
        for key, value in options.items():
            q &= Q(pipe, key) == value
        
        return pipe.select(where = q).remove()
    
    def hasRecord(self, record):
        """
        Checks to see if the given record is in the record set for this
        instance.
        
        :param      record | <orb.Table>
        
        :return     <bool>
        """
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return False
        
        where = self.query() & (Q(table) == record)
        return table.selectFirst(where = where) != None
    
    def removeRecord(self, record, **options):
        """
        Removes the record from this record set and from the database based
        on the piping information.
        
        :param      record | <orb.Table>
        
        :return     <int> | number of records removed
        """
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return 0
        
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        unique = options.pop('uniqueRecord', True)
        
        q  = Q(pipe, self._sourceColumn) == self._source
        q &= Q(pipe, self._targetColumn) == record
        
        for key, value in options.items():
            q &= Q(pipe, key) == value
        
        return pipe.select(where = q).remove()
    
    def remove(self, **options):
        """
        Removes all the links within this set.  This will remove
        from the Pipe table and not from the source record.
        """
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        q  = Q(pipe, self._sourceColumn) == self._source
        
        for key, value in options.items():
            q &= Q(pipe, key) == value
        
        return pipe.select(where = q).remove()
    
#----------------------------------------------------------------------

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
            return PipeRecordSet([], record)
        
        reload = options.get('reload', False)
        pipeTable = self.pipeReferenceModel()
        targetTable = self.targetReferenceModel()
        if None in (pipeTable, targetTable):
            return PipeRecordSet([], record)
        
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
        q  = Q(targetTable) == Q(pipeTable, self._targetColumn)
        q &= Q(pipeTable, self._sourceColumn) == record
        
        if 'where' in options:
            options['where'] = q & options['where']
        else:
            options['where'] = q
        
        # generate the new record set
        records = targetTable.select(**options)
        rset = PipeRecordSet(records,
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
        self.__name__ = nativestring(name)
    
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
        xpipe.set('cached',  nativestring(self.cached()))
        xpipe.set('expires', nativestring(self._cachedExpires))
        
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