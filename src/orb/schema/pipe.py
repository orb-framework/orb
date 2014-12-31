#!/usr/bin/python

""" Defines an piping system to use when accessing multi-to-multi records. """

# define authorship information
__authors__ = ['Eric Hulser']
__author__ = ','.join(__authors__)
__credits__ = []
__copyright__ = 'Copyright (c) 2011, Projex Software'
__license__ = 'LGPL'

# maintanence information
__maintainer__ = 'Projex Software'
__email__ = 'team@projexsoftware.com'

# ------------------------------------------------------------------------------

import projex.text

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

    def __init__(self, name, **options):
        self.__name__ = name
        self._pipeReference = options.get('pipeReference', options.get('through', ''))
        self._pipeTable = None
        self._sourceColumn = options.get('sourceColumn', options.get('source', ''))
        self._targetReference = options.get('targetReference', '')
        self._targetTable = None
        self._targetColumn = options.get('targetColumn', options.get('target', ''))
        self._cached = options.get('cached', False)
        self._cachedExpires = options.get('cachedExpires', 0)
        self._referenced = options.get('referenced', False)
        self._cache = {}

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
        sub_q = orb.Query(pipeTable, self._sourceColumn) == record
        rset = pipeTable.select(columns=[self._targetColumn], where=sub_q)
        q = orb.Query(targetTable).in_(rset)

        if 'where' in options:
            options['where'] = q & options['where']
        else:
            options['where'] = q

        # generate the new record set
        records = targetTable.select(**options)

        # map this recordset to a pipe set (if the options generate a pipe set)
        if isinstance(records, orb.RecordSet):
            rset_cls = record.schema().context(self.__name__).get('RecordSet', orb.PipeRecordSet)
            rset = rset_cls(records,
                            record,
                            pipeTable,
                            self._sourceColumn,
                            self._targetColumn)

            if pipe_cache:
                pipe_cache.setValue(cache_key, rset)
            if target_cache:
                target_cache.setValue(cache_key, rset)
        else:
            rset = records

        return rset

    def cache(self, table, force=False):
        """
        Returns the cache for the inputed table.
        
        :param      table | <subclass of orb.Table>
        
        :return     <orb.TableCache> || None
        """
        try:
            return self._cache[table]
        except KeyError:
            if force or self.cached():
                cache = orb.TableCache(table, self._cachedExpires)
                self._cache[table] = cache
                return cache
            else:
                return None

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

    def preload(self, record, records, options):
        """
        Preloads the inputed record and result values.

        :param      record  | <orb.Table>
                    records | [<dict>, ..]
                    options | <orb.LookupOptions>
        """
        target_model = self.targetReferenceModel()
        pipe_model = self.pipeReferenceModel()
        pipe_cache = self.cache(pipe_model, force=True)
        target_cache = self.cache(target_model, force=True)

        cache_key = (id(record),
                     hash(options or orb.LookupOptions()),
                     id(record.database()))

        records = [target_model(db_dict=record) for record in records or []]
        pset = orb.PipeRecordSet(orb.RecordSet(records),
                                 record,
                                 pipe_model,
                                 self._sourceColumn,
                                 self._targetColumn)

        pipe_cache.setValue(cache_key, pset)
        target_cache.setValue(cache_key, pset)

    def pipeReference(self):
        return self._pipeReference

    def pipeReferenceModel(self):
        if self._pipeTable is None:
            pipeTable = orb.system.model(self._pipeReference)
            if pipeTable and not self._targetReference:
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

    def schema(self):
        return self._schema

    def setSchema(self, schema):
        self._schema = schema

    def sourceColumn(self):
        return self._sourceColumn

    def sourceReferenceModel(self):
        model = self.pipeReferenceModel()
        column = model.schema().column(self.sourceColumn())
        return column.referenceModel()

    def targetColumn(self):
        return self._targetColumn

    def targetReference(self):
        if not self._targetReference:
            try:
                pipe = orb.system.schema(self.pipeReference())
                col = pipe.column(self.targetColumn())
                self._targetReference = col.reference()
            except AttributeError:
                pass

        return self._targetReference

    def targetReferenceModel(self):
        if self._targetTable is None:
            self._targetTable = orb.system.model(self.targetReference())
        return self._targetTable

    def toolTip(self, context='pipe'):
        tip = '''\
<b>{schema}.{name} <small>(RecordSet([{target_model}, ..]))</small></b>
<pre>
>>> # piped through {pipe} model
>>> {record} = {schema}()
>>> {record}.{getter}()
&lt;orb.PipeRecordSet([&lt;{target_model}&gt;, ..])&gt;

>>> # modify the joined records
>>> {target_record} = {target_model}.all().first()
>>> {record}.{getter}().addRecord({target_record})
>>> {record}.{getter}().removeRecord({target_record})
</pre>
'''
        return tip.format(name=self.name(),
                          getter=self.name(),
                          schema=self.schema().name(),
                          record=projex.text.underscore(self.schema().name()),
                          pipe=self.pipeReference(),
                          source=self.sourceColumn(),
                          target=self.targetColumn(),
                          target_model=self.targetReference(),
                          target_record=projex.text.underscore(self.targetReference()))

    def toXml(self, xparent):
        """
        Saves the index data for this column to XML.
        
        :param      xparent     | <xml.etree.ElementTree.Element>
        
        :return     <xml.etree.ElementTree.Element>
        """
        xpipe = ElementTree.SubElement(xparent, 'pipe')
        xpipe.set('name', self.name())

        if self.cached():
            xpipe.set('cached', nstr(self.cached()))
            xpipe.set('expires', nstr(self._cachedExpires))

        ElementTree.SubElement(xpipe, 'through').text = self._pipeReference
        ElementTree.SubElement(xpipe, 'source').text = self._sourceColumn
        ElementTree.SubElement(xpipe, 'target').text = self._targetColumn

        if self._targetReference:
            ElementTree.SubElement(xpipe, 'table').text = self._targetReference

        return xpipe

    @staticmethod
    def fromXml(xpipe, referenced=False):
        """
        Generates an index method descriptor from xml data.
        
        :param      xindex  | <xml.etree.Element>
        
        :return     <Index> || None
        """
        pipe = Pipe(xpipe.get('name', ''), referenced=referenced)
        pipe.setCached(xpipe.get('cached') == 'True')
        pipe.setCachedExpires(int(xpipe.get('expires', pipe._cachedExpires)))

        try:
            pipe.setPipeReference(xpipe.find('through').text)
        except AttributeError:
            pipe.setPipeReference(xpipe.get('pipeReference', ''))

        try:
            pipe.setSourceColumn(xpipe.find('source').text)
        except AttributeError:
            pipe.setSourceColumn(xpipe.get('sourceColumn', ''))

        try:
            pipe.setTargetReference(xpipe.find('table').text)
        except AttributeError:
            pipe.setTargetReference(xpipe.get('targetReference', ''))

        try:
            pipe.setTargetColumn(xpipe.find('target').text)
        except AttributeError:
            pipe.setTargetColumn(xpipe.get('targetColumn', ''))

        return pipe

